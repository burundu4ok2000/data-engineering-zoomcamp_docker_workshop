import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# 1. Добавили gcp_temp_bucket в аргументы, чтобы не хардкодить чужое облако
parser = argparse.ArgumentParser()
parser.add_argument("--input_green", required=True)
parser.add_argument("--input_yellow", required=True)
parser.add_argument("--output", required=True)
parser.add_argument(
    "--gcp_temp_bucket", required=True, help="Temporary GCS bucket for BigQuery staging"
)

args = parser.parse_args()

input_green = args.input_green
input_yellow = args.input_yellow
output = args.output
gcp_temp_bucket = args.gcp_temp_bucket

spark = SparkSession.builder.appName("taxi_revenue_aggregation").getOrCreate()

# Подставляем твой бакет
spark.conf.set("temporaryGcsBucket", gcp_temp_bucket)

# 2. Оставил возможность чтения вложенных папок (на случай, если пути без маски)
df_green = spark.read.option("recursiveFileLookup", "true").parquet(input_green)
df_green = df_green.withColumnRenamed(
    "lpep_pickup_datetime", "pickup_datetime"
).withColumnRenamed("lpep_dropoff_datetime", "dropoff_datetime")

df_yellow = spark.read.option("recursiveFileLookup", "true").parquet(input_yellow)
df_yellow = df_yellow.withColumnRenamed(
    "tpep_pickup_datetime", "pickup_datetime"
).withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime")

common_columns = [
    "VendorID",
    "pickup_datetime",
    "dropoff_datetime",
    "store_and_fwd_flag",
    "RatecodeID",
    "PULocationID",
    "DOLocationID",
    "passenger_count",
    "trip_distance",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "payment_type",
    "congestion_surcharge",
]

df_green_sel = df_green.select(common_columns).withColumn(
    "service_type", F.lit("green")
)

df_yellow_sel = df_yellow.select(common_columns).withColumn(
    "service_type", F.lit("yellow")
)

df_trips_data = df_green_sel.unionAll(df_yellow_sel)

# 3. Обновленный синтаксис Spark 4.x
df_trips_data.createOrReplaceTempView("trips_data")

# 4. Строгая типизация для финансовых метрик
df_result = spark.sql("""
SELECT 
    -- Revenue grouping 
    PULocationID AS revenue_zone,
    date_trunc('month', pickup_datetime) AS revenue_month, 
    service_type, 

    -- Revenue calculation (Clean Money)
    CAST(SUM(fare_amount) AS DECIMAL(10,2)) AS revenue_monthly_fare,
    CAST(SUM(extra) AS DECIMAL(10,2)) AS revenue_monthly_extra,
    CAST(SUM(mta_tax) AS DECIMAL(10,2)) AS revenue_monthly_mta_tax,
    CAST(SUM(tip_amount) AS DECIMAL(10,2)) AS revenue_monthly_tip_amount,
    CAST(SUM(tolls_amount) AS DECIMAL(10,2)) AS revenue_monthly_tolls_amount,
    CAST(SUM(improvement_surcharge) AS DECIMAL(10,2)) AS revenue_monthly_improvement_surcharge,
    CAST(SUM(total_amount) AS DECIMAL(10,2)) AS revenue_monthly_total_amount,
    CAST(SUM(congestion_surcharge) AS DECIMAL(10,2)) AS revenue_monthly_congestion_surcharge,

    -- Additional calculations
    AVG(passenger_count) AS avg_monthly_passenger_count,
    AVG(trip_distance) AS avg_monthly_trip_distance
FROM
    trips_data
GROUP BY
    1, 2, 3
""")

df_result.write.format("bigquery").option("table", output).mode("overwrite").save()
