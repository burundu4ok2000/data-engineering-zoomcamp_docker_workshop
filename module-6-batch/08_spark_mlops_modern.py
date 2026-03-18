import marimo

__generated_with = "0.20.4"
app = marimo.App(width="medium")


@app.cell
def _():
    from pyspark.sql import SparkSession
    import pandas as pd
    from typing import Iterator
    import os

    return Iterator, SparkSession, os, pd


@app.cell
def _(SparkSession):
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName('mlops_arrow') \
        .config("spark.driver.memory", "6g") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .config("spark.sql.execution.arrow.maxRecordsPerBatch", "5000") \
        .getOrCreate()
    return (spark,)


@app.cell
def _(os, spark):
    green_path = os.path.abspath("data/pq/green/")
    yellow_path = os.path.abspath("data/pq/yellow/")

    # 1. Твоя детальная проверка обоих путей
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
            print(f"❌ ОШИБКА: Директории {name} не существует.\n")

    # 2. Загрузка Green и подготовка фичей (нужно для следующих ячеек)
    if os.path.exists(green_path):
        df_green = spark.read.option("recursiveFileLookup", "true").parquet(green_path)
    
        # Сразу отбираем нужные колонки для нашего ML-конвейера
        feature_columns = ['VendorID', 'lpep_pickup_datetime', 'PULocationID', 'DOLocationID', 'trip_distance']
        df_features = df_green.select(feature_columns)
    
        print(f"🚀 Слой Silver готов. Подготовлено: {df_features.count()} строк для инференса.")
    else:
        df_features = None
    return (df_features,)


@app.cell
def _(Iterator, pd):
    def apply_model_in_pandas(iterator: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
        for pdf in iterator:
            # Векторизованная операция на уровне C/C++ через Pandas/Arrow
            pdf['predicted_duration'] = pdf['trip_distance'] * 5.0
            yield pdf

    return (apply_model_in_pandas,)


@app.cell
def _(apply_model_in_pandas, df_features):
    # Строгая схема, которую ожидает Spark на выходе
    output_schema = "VendorID int, lpep_pickup_datetime timestamp, PULocationID int, DOLocationID int, trip_distance double, predicted_duration double"

    df_predicts = df_features.mapInPandas(
        apply_model_in_pandas,
        schema=output_schema
    )
    return (df_predicts,)


@app.cell
def _(df_predicts):
    df_predicts.select('trip_distance', 'predicted_duration').limit(15).toPandas()
    return


if __name__ == "__main__":
    app.run()
