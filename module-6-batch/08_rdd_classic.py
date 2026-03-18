import marimo

__generated_with = "0.20.4"
app = marimo.App(width="medium")


@app.cell
def _():
    from pyspark.sql import SparkSession
    from pyspark.sql import types
    from datetime import datetime
    from collections import namedtuple
    import pandas as pd

    return SparkSession, datetime, types


@app.cell
def _(SparkSession):
    spark = SparkSession.builder \
            .master("local[*]") \
            .appName('rdd_classic') \
            .config("spark.driver.memory", "4g") \
            .getOrCreate()
    return (spark,)


@app.cell
def _():
    import os

    green_path = os.path.abspath("data/pq/green/")
    yellow_path = os.path.abspath("data/pq/yellow/")

    # Проверяем обе папки в цикле, чтобы не дублировать код
    for name, path in [("GREEN", green_path), ("YELLOW", yellow_path)]:
        print(f"--- Проверка {name} ---")
        print(f"Абсолютный путь: {path}")

        if os.path.exists(path):
            files = []
            for root, dirs, filenames in os.walk(path):
                for f in filenames:
                    if f.endswith(".parquet"):
                        files.append(os.path.join(root, f))

            if not files:
                print(f"❌ ОШИБКА: В папке {name} нет ни одного .parquet файла!\n")
            else:
                print(f"✅ Найдено файлов: {len(files)}")
                print(f"Пример первого файла: {files[0]}\n")
        else:
            print(f"❌ ОШИБКА: Самой директории {name} не существует по этому пути.\n")
    return (green_path,)


@app.cell
def _(green_path, spark):
    df_green = spark.read.option("recursiveFileLookup", "true").parquet(green_path)

    # Проверяем результат Green
    print(f"Загружено строк Green: {df_green.count()}")
    df_green.printSchema()
    df_green.show(5)
    return (df_green,)


@app.cell
def _():
    from decimal import Decimal

    return (Decimal,)


@app.cell
def _(datetime):
    # 1. Задаем дату для фильтра
    start_date = datetime(year=2020, month=1, day=1)
    return (start_date,)


@app.cell
def _(df_green):
    # 2. Вытаскиваем RDD
    rdd = df_green.select('lpep_pickup_datetime', 'PULocationID', 'total_amount').rdd
    return (rdd,)


@app.cell
def _(types):
    # 3. Схема 
    result_schema = types.StructType([
        types.StructField('hour', types.TimestampType(), True),
        types.StructField('zone', types.IntegerType(), True),
        types.StructField('revenue', types.DecimalType(10, 2), True),
        types.StructField('count', types.IntegerType(), True)
    ])
    return (result_schema,)


@app.cell
def _(start_date):
    def filter_outliers(row):
        return row.lpep_pickup_datetime >= start_date

    return (filter_outliers,)


@app.function
def prepare_for_grouping(row):
    hour = row.lpep_pickup_datetime.replace(minute=0, second=0, microsecond=0)
    zone = row.PULocationID
    key = (hour, zone)
    
    amount = row.total_amount
    count = 1
    value = (amount, count)
    
    return (key, value)


@app.function
def calculate_revenue(left_value, right_value):
    left_amount, left_count = left_value
    right_amount, right_count = right_value
    
    output_amount = left_amount + right_amount
    output_count = left_count + right_count
    
    return (output_amount, output_count)


@app.cell
def _(Decimal):
    def unwrap(kv):
        key, value = kv
        hour = key[0]
        zone = key[1]
    
        amount = value[0]
        count = value[1]
    
        # Конвертация в Decimal с точными копейками
        revenue = Decimal(str(amount)).quantize(Decimal('0.01'))
    
        return (hour, zone, revenue, count)

    return (unwrap,)


@app.cell
def _(filter_outliers, rdd, result_schema, unwrap):
    # Итоговый читаемый пайплайн
    df_result = rdd \
        .filter(filter_outliers) \
        .map(prepare_for_grouping) \
        .reduceByKey(calculate_revenue) \
        .map(unwrap) \
        .toDF(result_schema)
    return (df_result,)


@app.cell
def _(df_result):
    # Лимитируем вывод, чтобы не перегружать память браузера, и отдаем в pandas
    df_result.limit(50).toPandas()
    return


if __name__ == "__main__":
    app.run()
