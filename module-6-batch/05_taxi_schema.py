import marimo

__generated_with = "0.20.4"
app = marimo.App(width="medium")


@app.cell
def _():
    import pyspark
    from pyspark.sql import SparkSession

    return (SparkSession,)


@app.cell
def _(SparkSession):
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName('test') \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()
    return (spark,)


@app.cell
def _():
    import os
    import requests
    from pathlib import Path

    def download_taxi_data(services, years):
        base_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download"
    
        for service in services:
            for year in years:
                for month in range(1, 13):
                    # Формируем имя файла и URL (например, green_tripdata_2020-01.csv.gz)
                    file_name = f"{service}_tripdata_{year}-{month:02d}.csv.gz"
                    url = f"{base_url}/{service}/{file_name}"
                
                    # Путь, который ожидает Спарк в твоем ноутбуке
                    target_dir = Path(f"data/raw/{service}/{year}/{month:02d}")
                    target_dir.mkdir(parents=True, exist_ok=True)
                    target_path = target_dir / file_name
                
                    if target_path.exists():
                        print(f"✅ {file_name} уже на диске")
                        continue
                
                    print(f"📥 Качаю {file_name}...")
                    response = requests.get(url, stream=True)
                    if response.status_code == 200:
                        with open(target_path, "wb") as f:
                            for chunk in response.iter_content(chunk_size=8192):
                                f.write(chunk)
                    else:
                        print(f"❌ Ошибка {response.status_code} для {file_name} (возможно, данных за этот месяц нет)")

    # Запускаем загрузку для Green и Yellow такси за 2020-2021
    download_taxi_data(services=["green", "yellow"], years=[2020, 2021])
    return Path, os


@app.cell
def _():
    from pyspark.sql import types

    green_schema = types.StructType([
        types.StructField("VendorID", types.IntegerType(), True),
        types.StructField("lpep_pickup_datetime", types.TimestampType(), True),
        types.StructField("lpep_dropoff_datetime", types.TimestampType(), True),
        types.StructField("store_and_fwd_flag", types.StringType(), True),
        types.StructField("RatecodeID", types.IntegerType(), True),
        types.StructField("PULocationID", types.IntegerType(), True),
        types.StructField("DOLocationID", types.IntegerType(), True),
        types.StructField("passenger_count", types.IntegerType(), True),
        types.StructField("trip_distance", types.DoubleType(), True),
        types.StructField("fare_amount", types.DoubleType(), True),
        types.StructField("extra", types.DoubleType(), True),
        types.StructField("mta_tax", types.DoubleType(), True),
        types.StructField("tip_amount", types.DoubleType(), True),
        types.StructField("tolls_amount", types.DoubleType(), True),
        types.StructField("ehail_fee", types.DoubleType(), True),
        types.StructField("improvement_surcharge", types.DoubleType(), True),
        types.StructField("total_amount", types.DoubleType(), True),
        types.StructField("payment_type", types.IntegerType(), True),
        types.StructField("trip_type", types.IntegerType(), True),
        types.StructField("congestion_surcharge", types.DoubleType(), True)
    ])

    yellow_schema = types.StructType([
        types.StructField("VendorID", types.IntegerType(), True),
        types.StructField("tpep_pickup_datetime", types.TimestampType(), True),
        types.StructField("tpep_dropoff_datetime", types.TimestampType(), True),
        types.StructField("passenger_count", types.IntegerType(), True),
        types.StructField("trip_distance", types.DoubleType(), True),
        types.StructField("RatecodeID", types.IntegerType(), True),
        types.StructField("store_and_fwd_flag", types.StringType(), True),
        types.StructField("PULocationID", types.IntegerType(), True),
        types.StructField("DOLocationID", types.IntegerType(), True),
        types.StructField("payment_type", types.IntegerType(), True),
        types.StructField("fare_amount", types.DoubleType(), True),
        types.StructField("extra", types.DoubleType(), True),
        types.StructField("mta_tax", types.DoubleType(), True),
        types.StructField("tip_amount", types.DoubleType(), True),
        types.StructField("tolls_amount", types.DoubleType(), True),
        types.StructField("improvement_surcharge", types.DoubleType(), True),
        types.StructField("total_amount", types.DoubleType(), True),
        types.StructField("congestion_surcharge", types.DoubleType(), True)
    ])

    return green_schema, yellow_schema


@app.cell
def _(green_schema, os, spark, yellow_schema):

    # Схемы мы уже задали в прошлых ячейках (green_schema, yellow_schema)

    datasets = {
        "green": green_schema,
        "yellow": yellow_schema
    }

    for service, schema in datasets.items():
        for year in [2020, 2021]:
            for month in range(1, 13):
                input_path = f'data/raw/{service}/{year}/{month:02d}/'
                output_path = f'data/pq/{service}/{year}/{month:02d}/'

                # 1. Проверяем, есть ли что обрабатывать
                if not os.path.exists(input_path) or not os.listdir(input_path):
                    print(f"⏩ Пропускаем {service} {year}/{month:02d}: файлов нет")
                    continue

                # 2. Проверяем нашу защиту _SUCCESS (чтобы не пересчитывать готовое)
                if os.path.exists(os.path.join(output_path, "_SUCCESS")):
                    print(f"✅ {service} {year}/{month:02d} уже обработан. Пропуск.")
                    continue

                print(f"⚙️ Обработка {service} {year}/{month:02d}...")
            
                df = spark.read \
                    .option("header", "true") \
                    .schema(schema) \
                    .csv(input_path)

                # Используем все твои 8 ядер (repartition 8)
                df.repartition(8).write.parquet(output_path, mode="overwrite")
    return


@app.cell
def _(Path):
    def get_size_stats(root_path):
        total_bytes = 0
        file_count = 0
        for p in Path(root_path).rglob('*'):
            if p.is_file():
                total_bytes += p.stat().st_size
                file_count += 1
        return total_bytes / (1024 * 1024), file_count

    # Считаем Бронзу (CSV.GZ)
    raw_size, raw_files = get_size_stats('data/raw')
    # Считаем Серебро (Parquet)
    pq_size, pq_files = get_size_stats('data/pq')

    print(f"📊 АУДИТ ХРАНИЛИЩА:")
    print(f"{'---':<20} | {'Размер (MB)':<12} | {'Кол-во файлов':<12}")
    print(f"{'Bronze (CSV.GZ)':<20} | {raw_size:<12.2f} | {raw_files:<12}")
    print(f"{'Silver (Parquet)':<20} | {pq_size:<12.2f} | {pq_files:<12}")
    print(f"---")
    print(f"💡 Коэффициент изменения объема: {pq_size/raw_size:.2f}x")
    return


if __name__ == "__main__":
    app.run()
