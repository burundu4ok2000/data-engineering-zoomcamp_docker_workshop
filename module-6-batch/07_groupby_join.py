import marimo

__generated_with = "0.20.4"
app = marimo.App(width="medium")


@app.cell
def _():
    import pyspark  # noqa: F401
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
    return green_path, yellow_path


@app.cell
def _(green_path, spark):
    df_green = spark.read.option("recursiveFileLookup", "true").parquet(green_path)

    # Проверяем результат Green
    print(f"Загружено строк Green: {df_green.count()}")
    df_green.printSchema()
    df_green.show(5)
    return (df_green,)


@app.cell
def _(df_green):
    # Было: df_green.registerTempTable('green')
    # Стало:
    df_green.createOrReplaceTempView("green")
    return


@app.cell
def _(spark):
    df_green_revenue = spark.sql("""
    SELECT 
        date_trunc('hour', lpep_pickup_datetime) AS hour, 
        PULocationID AS zone,

        CAST(SUM(total_amount) AS DECIMAL(10,2)) AS amount,
        COUNT(1) AS number_records
    FROM
        green
    WHERE
        lpep_pickup_datetime >= '2020-01-01 00:00:00'
    GROUP BY
        1, 2
    """)
    return (df_green_revenue,)


@app.cell
def _(df_green_revenue):
    df_green_revenue.repartition(20).write.parquet(
        "data/report/revenue/green", mode="overwrite"
    )
    return


@app.cell
def _(spark, yellow_path):
    df_yellow = spark.read.option("recursiveFileLookup", "true").parquet(yellow_path)

    # Проверяем результат Yellow
    print(f"Загружено строк Yellow: {df_yellow.count()}")
    df_yellow.printSchema()
    df_yellow.show(5)
    return (df_yellow,)


@app.cell
def _(df_yellow):
    df_yellow.createOrReplaceTempView("yellow")
    return


@app.cell
def _(spark):
    df_yellow_revenue = spark.sql("""
    SELECT 
        date_trunc('hour', tpep_pickup_datetime) AS hour, 
        PULocationID AS zone,

        CAST(SUM(total_amount) AS DECIMAL(10,2)) AS amount,
        COUNT(1) AS number_records
    FROM
        yellow
    WHERE
        tpep_pickup_datetime >= '2020-01-01 00:00:00'
    GROUP BY
        1, 2
    """)
    return (df_yellow_revenue,)


@app.cell
def _(df_yellow_revenue):
    df_yellow_revenue.repartition(20).write.parquet(
        "data/report/revenue/yellow", mode="overwrite"
    )
    return


@app.cell
def _(df_green_revenue, df_yellow_revenue):
    # 1. Подготавливаем Green (переименовываем колонки, чтобы не путаться при джоине)
    df_green_revenue_tmp = df_green_revenue.withColumnRenamed(
        "amount", "green_amount"
    ).withColumnRenamed("number_records", "green_number_records")

    # 2. Подготавливаем Yellow
    df_yellow_revenue_tmp = df_yellow_revenue.withColumnRenamed(
        "amount", "yellow_amount"
    ).withColumnRenamed("number_records", "yellow_number_records")

    # 3. Делаем тот самый мощный OUTER JOIN
    # Он объединит данные по часам и зонам.
    # Если в этот час в этой зоне было только зеленое такси — желтые колонки будут NULL, и наоборот.
    df_join = df_green_revenue_tmp.join(
        df_yellow_revenue_tmp, on=["hour", "zone"], how="outer"
    )
    return df_green_revenue_tmp, df_join, df_yellow_revenue_tmp


@app.cell
def _(df_green_revenue_tmp, df_yellow_revenue_tmp):
    import pyspark.sql.functions as F

    print("--- Проверка перекоса в Green Taxi ---")
    df_green_revenue_tmp.groupBy("hour", "zone").count().orderBy(
        F.col("count").desc()
    ).show(5)

    print("--- Проверка перекоса в Yellow Taxi ---")
    df_yellow_revenue_tmp.groupBy("hour", "zone").count().orderBy(
        F.col("count").desc()
    ).show(5)
    return


@app.cell
def _(df_join):
    # 1. Считываем сохраненный тотальный отчет
    df_join.write.parquet("data/report/revenue/total", mode="overwrite")
    return


@app.cell
def _(df_join, spark):
    # 2. Считываем справочник зон (проверь, как называется твоя папка/файл)
    # Если у тебя CSV, используй: spark.read.csv('taxi+_zone_lookup.csv', header=True, inferSchema=True)
    df_zones = spark.read.parquet("zones/")

    # 3. Делаем финальный JOIN со справочником
    df_result = df_join.join(df_zones, df_join.zone == df_zones.LocationID)

    # 4. Дропаем дублирующиеся колонки с ID и сохраняем финал
    df_result.drop("LocationID", "zone").write.parquet(
        "tmp/revenue-zones", mode="overwrite"
    )

    # Смотрим на результат!
    df_result.show(5)
    return


@app.cell
def _(spark):
    spark.stop()
    return


if __name__ == "__main__":
    app.run()
