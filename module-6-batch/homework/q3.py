import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def main():
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName('homework_q3') \
        .getOrCreate()
        
    data_dir = "homework/pq/yellow_tripdata_2025_11"
    
    if os.path.exists(data_dir):
        df = spark.read.parquet(data_dir)
        print("Using repartitioned data from Q2")
    else:
        df = spark.read.parquet("homework/data/yellow_tripdata_2025-11.parquet")
        print("Using original downloaded data")
        
    # Count records on 15th of November
    # Yellow taxi schema has "tpep_pickup_datetime"
    df.registerTempTable("yellow")
    
    count = spark.sql("""
        SELECT count(1) 
        FROM yellow
        WHERE date(tpep_pickup_datetime) = '2025-11-15'
    """).collect()[0][0]
    
    print(f"Trips on November 15, 2025: {count}")
    
if __name__ == "__main__":
    main()
