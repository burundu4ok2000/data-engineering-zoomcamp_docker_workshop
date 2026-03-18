import marimo

__generated_with = "0.20.4"
app = marimo.App(width="medium")


@app.cell
def _():
    from pyspark.sql import SparkSession
    import pandas as pd
    from typing import Iterator
    import os

    return SparkSession, os, pd


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
    return (df_green,)


@app.cell
def _(df_green):
    # Ячейка 1: Колонки и создание RDD для ML
    columns = ['VendorID', 'lpep_pickup_datetime', 'PULocationID', 'DOLocationID', 'trip_distance']

    duration_rdd = df_green \
        .select(columns) \
        .rdd
    return columns, duration_rdd


@app.function
# Ячейка 2: Наша "Супер-нейросеть" (Заглушка)

def model_predict(df):
    # В реальности здесь было бы: return model.predict(df)
    # Наша заглушка: 5 минут на каждую милю дистанции
    y_pred = df.trip_distance * 5
    return y_pred


@app.cell
def _(columns, pd):
    # Ячейка 3: Функция батчинга (Тот самый исторический артефакт)

    def apply_model_in_batch(rows):
        # Разворачиваем итератор в тяжелый Pandas DataFrame
        df = pd.DataFrame(rows, columns=columns)
    
        # Скармливаем батч модели
        predictions = model_predict(df)
    
        # Приклеиваем колонку с предсказаниями
        df['predicted_duration'] = predictions

        # Сворачиваем обратно в итератор через itertuples()
        for row in df.itertuples():
            yield row

    return (apply_model_in_batch,)


@app.cell
def _(apply_model_in_batch, duration_rdd):
    # Ячейка 4: Запуск инференса и возврат в Spark DataFrame
    df_predicts = duration_rdd \
        .mapPartitions(apply_model_in_batch) \
        .toDF() \
        .drop('Index')

    # Смотрим результат работы нашей модели
    df_predicts.select('predicted_duration').show()
    return


if __name__ == "__main__":
    app.run()
