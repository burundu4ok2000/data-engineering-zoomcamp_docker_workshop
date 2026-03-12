import marimo

__generated_with = "0.20.4"
app = marimo.App(width="medium")


@app.cell
def _():
    from pyspark.sql import SparkSession

    return (SparkSession,)


@app.cell
def _(SparkSession):
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("test")
        .config("spark.driver.memory", "4g")
        .getOrCreate()
    )
    return (spark,)


@app.cell
def _():
    import os
    import requests
    from pathlib import Path

    def download_file(url: str, destination: str):
        Path(os.path.dirname(destination)).mkdir(parents=True, exist_ok=True)

        if not os.path.exists(destination):
            print(f"📥 Начинаю загрузку: {url}")
            with requests.get(url, stream=True) as r:
                r.raise_for_status()
                with open(destination, "wb") as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
            print(f"✅ Файл сохранен: {destination}")
        else:
            print(f"🆗 Файл уже на месте: {destination}")

    # ---------------------------------------------------------
    # МЕНЯЕМ ТОЛЬКО ЭТОТ БЛОК:
    # ---------------------------------------------------------
    url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhvhv/fhvhv_tripdata_2021-01.csv.gz"
    output = "data/fhvhv_tripdata_2021-01.csv.gz"

    download_file(url, output)
    return (Path,)


@app.cell
def _():
    import gzip

    file_path = "data/fhvhv_tripdata_2021-01.csv.gz"

    # 'rt' означает Read Text - Питон на лету сам разжимает байты в текст
    with gzip.open(file_path, "rt", encoding="utf-8") as f:
        for _ in range(5):
            print(f.readline().strip())
    return file_path, gzip


@app.cell
def _(file_path, gzip):
    # Нам не нужно заново делать import gzip или задавать file_path,
    # они уже доступны из твоей предыдущей ячейки!

    def count_lines_in_gz(path: str) -> int:
        print(f"🕵️ Начинаю подсчет строк в архиве {path}...")
        count = 0
        with (
            gzip.open(path, "rt", encoding="utf-8") as f
        ):  # Используем локальное имя f - недоступное вне функции count_lines_in_gz()
            for _ in f:
                count += 1
        return count

    # Вызываем функцию, передавая ей уже существующий file_path
    total_lines = count_lines_in_gz(file_path)
    print(f"✅ Готово! Эталонное количество строк: {total_lines}")
    return


@app.cell
def _(spark):
    # 1. Читаем тяжелую конфету (Спарк будет пыхтеть на 1 потоке)
    df_fhvhv = spark.read.option("header", "true").csv(
        "data/fhvhv_tripdata_2021-01.csv.gz"
    )

    # 2. Мгновенно конвертируем в легкоусвояемый Parquet
    # (Спарк сам разобьет данные на удобные куски)
    print("Начинаю перегонку в Parquet. Это займет пару минут...")

    df_fhvhv.write.mode("overwrite").parquet("data/fhvhv_2021_01_parquet")

    print("✅ Данные успешно переведены в Parquet!")
    return


@app.cell
def _(spark):
    # Эта команда возвращает точный локальный URL активного интерфейса
    spark.sparkContext.uiWebUrl
    return


@app.cell
def _(spark):
    # Читаем серебряный слой
    df_silver = spark.read.parquet("data/fhvhv_2021_01_parquet")

    # Запускаем экшен!
    df_silver.count()
    return (df_silver,)


@app.cell
def _():
    from pyspark.sql import types

    return (types,)


@app.cell
def _(types):
    # 1. Задаем строгую схему (Ревью кода пройдено: типы оптимальны)
    schema = types.StructType(
        [
            types.StructField("hvfhs_license_num", types.StringType(), True),
            types.StructField("dispatching_base_num", types.StringType(), True),
            types.StructField("pickup_datetime", types.TimestampType(), True),
            types.StructField("dropoff_datetime", types.TimestampType(), True),
            types.StructField(
                "PULocationID", types.IntegerType(), True
            ),  # Экономим память!
            types.StructField(
                "DOLocationID", types.IntegerType(), True
            ),  # Экономим память!
            types.StructField("SR_Flag", types.StringType(), True),
        ]
    )
    return (schema,)


@app.cell
def _(Path, schema, spark):

    output_path = "data/fhvhv/2021/01/"
    # Ищем тот самый маркер успешной записи
    success_marker = Path(output_path) / "_SUCCESS"

    if success_marker.exists():
        print(f"🆗 Серебряный слой уже существует ({output_path}).")
        print("Тяжелая перегонка пропущена! Экономим ресурсы сервера.")
    else:
        print("Читаем архив со строгой схемой...")
        df = (
            spark.read.option("header", "true")
            .schema(schema)
            .csv("data/fhvhv_tripdata_2021-01.csv.gz")
        )

        print("Запускаем shuffle и рубим на 24 партиции...")
        df_repartitioned = df.repartition(24)

        print(f"Пишем Parquet в {output_path}...")

        # Оставляем overwrite на случай, если прошлый запуск упал на середине
        df_repartitioned.write.mode("overwrite").parquet(output_path)

        print("✅ Готово! Бронза успешно переплавлена в Серебро.")
    return


@app.cell
def _(spark):
    # Проверка выделенной памяти (в байтах)
    sc = spark.sparkContext
    print(f"Текущий лимит Driver Memory: {sc._conf.get('spark.driver.memory')}")
    return


@app.cell
def _(df_silver):
    # 1. Импортируем функции
    from pyspark.sql import functions as F

    # 2. Регистрируем таблицу для SQL (если еще не сделал)
    df_silver.createOrReplaceTempView("trips_data")

    # 3. Делаем трансформации "по-взрослому"
    df_transformed = (
        df_silver.withColumn("pickup_date", F.to_date(df_silver.pickup_datetime))
        .withColumn("dropoff_date", F.to_date(df_silver.dropoff_datetime))
        .select(
            "dispatching_base_num",
            "pickup_date",
            "dropoff_date",
            "PULocationID",
            "DOLocationID",
        )
    )

    df_transformed.show(5)
    return (F,)


@app.cell
def _(F, df_silver):

    # Мы не пишем def crazy_stuff!
    # Мы собираем конструктор из встроенных функций Спарка:

    # 1. Отрезаем букву и превращаем в Integer
    base_num_int = F.substring(df_silver.dispatching_base_num, 2, 100).cast("integer")

    # 2. Описываем логику условий (аналог if/elif/else) через F.when
    df_pro = df_silver.withColumn(
        "base_id",
        F.when(
            base_num_int % 7 == 0, F.concat(F.lit("s/"), F.hex(base_num_int))
        )  # HEX прямо в Спарке!
        .when(base_num_int % 3 == 0, F.concat(F.lit("a/"), F.hex(base_num_int)))
        .otherwise(F.concat(F.lit("e/"), F.hex(base_num_int))),
    )

    # Выводим результат
    df_pro.select("dispatching_base_num", "base_id").show(5)
    return


if __name__ == "__main__":
    app.run()
