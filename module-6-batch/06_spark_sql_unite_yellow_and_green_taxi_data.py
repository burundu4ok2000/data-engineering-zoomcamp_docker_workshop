import marimo

__generated_with = "0.20.4"
app = marimo.App(width="medium")


@app.cell
def _():
    import pyspark  # noqa: F401

    return


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

    # Проверяем текущую папку
    print(f"📍 Я сейчас здесь: {os.getcwd()}")

    # Проверяем, видит ли система папки месяцев
    path = "data/pq/green/"
    if os.path.exists(path):
        years = os.listdir(path)
        print(f"📅 Вижу годы: {years}")
        for y in years:
            months = os.listdir(os.path.join(path, y))
            print(f"  └─ В {y} вижу месяцы: {months}...")
    else:
        print("❌ Папка data/pq/green не найдена! Проверь путь.")
    return


@app.cell
def _(spark):
    # Загружаем Green такси (Spark 4.1.1 Style)
    df_green = (
        spark.read.option("recursiveFileLookup", "true")
        .option(
            "basePath", "data/pq/green/"
        )  # Это важно, чтобы Spark сохранил структуру папок как колонки
        .parquet("data/pq/green/")
    )

    # Загружаем Yellow такси
    df_yellow = (
        spark.read.option("recursiveFileLookup", "true")
        .option("basePath", "data/pq/yellow/")
        .parquet("data/pq/yellow/")
    )

    print(f"✅ Данные загружены. Всего строк: {df_green.count() + df_yellow.count()}")
    return df_green, df_yellow


@app.cell
def _(df_green, df_yellow):
    # --- ШАГ 2: Регистрация "Staging" (как в dbt) ---
    df_green.createOrReplaceTempView("stg_green_tripdata")
    df_yellow.createOrReplaceTempView("stg_yellow_tripdata")
    return


@app.cell
def _(df_green, df_yellow, spark):
    # Убедись, что эти представления созданы из "чистых" паркетов
    df_green.createOrReplaceTempView("stg_green_tripdata")
    df_yellow.createOrReplaceTempView("stg_yellow_tripdata")

    trips_unioned_sql = """
    WITH green_tripdata AS (
        SELECT
            VendorID as vendor_id,
            lpep_pickup_datetime as pickup_datetime,  -- Мапим оригинальное имя в общее
            lpep_dropoff_datetime as dropoff_datetime,
            PULocationID as pickup_location_id,
            DOLocationID as dropoff_location_id,
            passenger_count,
            trip_distance,
            fare_amount,
            extra,
            mta_tax,
            tip_amount,
            tolls_amount,
            improvement_surcharge,
            total_amount,
            payment_type,
            congestion_surcharge,
            trip_type, 
            'Green' as service_type
        FROM stg_green_tripdata
    ),

    yellow_tripdata AS (
        SELECT
            VendorID as vendor_id,
            tpep_pickup_datetime as pickup_datetime,  -- Мапим оригинальное имя в общее
            tpep_dropoff_datetime as dropoff_datetime,
            PULocationID as pickup_location_id,
            DOLocationID as dropoff_location_id,
            passenger_count,
            trip_distance,
            fare_amount,
            extra,
            mta_tax,
            tip_amount,
            tolls_amount,
            improvement_surcharge,
            total_amount,
            payment_type,
            congestion_surcharge,
            CAST(1 AS INTEGER) AS trip_type,  -- Твой изящный хак из dbt
            'Yellow' as service_type
        FROM stg_yellow_tripdata
    )

    SELECT * FROM green_tripdata
    UNION ALL
    SELECT * FROM yellow_tripdata
    """

    df_trips_data = spark.sql(trips_unioned_sql)
    df_trips_data.createOrReplaceTempView("trips_data")

    # Sanity check
    spark.sql("SELECT service_type, count(*) FROM trips_data GROUP BY 1").show()
    return


@app.cell
def _(spark):
    import pandas as pd  # noqa: F401

    # 1. Выполняем агрегацию с форматированием прямо "в движке"
    # Мы сразу округляем суммы и приводим дату к красивому виду YYYY-MM
    df_result = spark.sql("""
    SELECT 
        pickup_location_id AS zone,
        date_format(date_trunc('month', pickup_datetime), 'yyyy-MM') AS month, 
        service_type AS type, 

        -- Финансы (округляем до центов)
        ROUND(SUM(fare_amount), 2) AS fare,
        ROUND(SUM(tip_amount), 2) AS tips,
        ROUND(SUM(total_amount), 2) AS total,

        -- Статистика (округляем до 1 знака)
        ROUND(AVG(passenger_count), 1) AS avg_passengers,
        ROUND(AVG(trip_distance), 2) AS avg_distance
    FROM
        trips_data
    GROUP BY
        1, 2, 3
    ORDER BY 
        1, 2, month DESC -- Свежие данные сверху внутри зоны
    """)

    # 2. Делаем "Бриллиантовый вывод"
    pretty_report = df_result.limit(50).toPandas()

    # Наводим финальный лоск
    (
        pretty_report.style.format(
            {
                "fare": "${:,.2f}",
                "tips": "${:,.2f}",
                "total": "${:,.2f}",
                "avg_passengers": "{:.1f}",
                "avg_distance": "{:.2f} mi",
            }
        )
        .background_gradient(
            subset=["total"], cmap="YlGn"
        )  # Мягкий градиент от желтого к зеленому
        .hide(axis="index")  # Убираем технический индекс Pandas (0, 1, 2...)
        .set_properties(
            **{"text-align": "center", "padding": "10px"}
        )  # Центрируем всё и добавляем воздуха
        # Магия: красим название сервиса в его цвет
        .map(
            lambda x: (
                "color: #2d5a27; font-weight: bold;"
                if x == "Green"
                else "color: #8a6d3b; font-weight: bold;"
            ),
            subset=["type"],
        )
        .set_table_styles(
            [
                # Делаем шапку таблицы жирной и профессиональной
                {
                    "selector": "th",
                    "props": [
                        ("font-size", "11pt"),
                        ("background-color", "#f8f9fa"),
                        ("color", "#495057"),
                        ("text-transform", "uppercase"),
                    ],
                }
            ]
        )
        .set_caption("🚕 NYC TAXI REVENUE DATAMART: GOLD LAYER ANALYSIS")
    )
    return (df_result,)


@app.cell
def _(df_result):
    # .coalesce(1) собирает все кусочки в один файл, чтобы не плодить мусор
    (
        df_result.coalesce(1)
        .write.mode("overwrite")
        .parquet("data/gold/revenue_report_clean")
    )
    return


@app.cell
def _(spark):
    spark.sparkContext.uiWebUrl
    return


if __name__ == "__main__":
    app.run()
