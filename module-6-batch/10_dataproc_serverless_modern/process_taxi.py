import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def run_etl(input_path, bq_table):
    """
    =================================================================================
    РАЗДЕЛ 1: ИНИЦИАЛИЗАЦИЯ SPARK (Углеродная vs Кремниевая формы жизни)
    =================================================================================
    В локальных скриптах (Marimo) ты писал: .master("local[*]").config("spark.driver.memory", "4g")

    ЗДЕСЬ ЭТОГО НЕТ. Почему?
    Потому что в Serverless-архитектуре ты (Углеродная форма) отдаешь управление железом
    Кремниевой форме (Google).

    FinOps-магия: Dataproc Serverless использует динамическое выделение ресурсов (Dynamic Allocation).
    Если на этапе Shuffle Спарку понадобится больше памяти, Google сам поднимет новые
    контейнеры (за миллисекунды), а как только нагрузка спадет — убьет их.
    Ты платишь ТОЛЬКО за потребленные DCU (Dataproc Compute Units) посекундно.
    Никакой оплаты за простой кластера!
    """
    spark = SparkSession.builder.appName("YellowTaxi_Serverless_ETL").getOrCreate()

    # Оставляем только важные предупреждения, чтобы не платить за хранение мусорных логов в Cloud Logging
    spark.sparkContext.setLogLevel("WARN")

    print(f"🚀 Старт ETL-процесса. Читаем данные из: {input_path}")

    """
    =================================================================================
    РАЗДЕЛ 2: ЧТЕНИЕ ДАННЫХ (Data Locality)
    =================================================================================
    В старых версиях Спарка (лекции 2022 года) тебе пришлось бы качать .jar файлы 
    для коннекта к Google Cloud Storage (GCS).
    В Dataproc Serverless коннекторы вшиты в ядро. Мы просто передаем путь gs://...
    """
    df_yellow = spark.read.parquet(input_path)

    """
    =================================================================================
    РАЗДЕЛ 3: ТРАНСФОРМАЦИЯ И ОЧИСТКА (Бизнес-логика)
    =================================================================================
    Берем логику из твоих старых скриптов. Сначала отсекаем грязные данные.
    Мы не тащим в Data Warehouse поездки-призраки (дистанция 0 или без пассажиров).
    """
    df_cleaned = df_yellow.filter(
        (F.col("trip_distance") > 0)
        & (F.col("passenger_count") > 0)
        & (F.col("fare_amount") > 0)  # Таксисты не работают бесплатно
    )

    # Приводим названия колонок к единому стандарту (как ты делал в dbt/прошлых скриптах)
    df_standard = df_cleaned.withColumnRenamed(
        "tpep_pickup_datetime", "pickup_datetime"
    ).withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime")

    # Добавляем хак: сервисную колонку, чтобы потом было легко джоинить с Green Taxi
    df_standard = df_standard.withColumn("service_type", F.lit("Yellow"))

    print(
        f"📊 Аудит качества: Было строк -> {df_yellow.count()} | Стало чистых -> {df_standard.count()}"
    )

    """
    =================================================================================
    РАЗДЕЛ 4: АГРЕГАЦИЯ ЧЕРЕЗ SPARK SQL (Опционально, но мощно)
    =================================================================================
    Создаем временную таблицу в оперативной памяти (TempView), чтобы использовать 
    классический SQL. Serverless-движок отлично оптимизирует SQL-запросы через 
    свой внутренний Catalyst Optimizer.
    """
    df_standard.createOrReplaceTempView("trips_data")

    # Считаем базовую статистику по зонам (адаптация твоего скрипта агрегации выручки)
    df_result = spark.sql("""
        SELECT 
            PULocationID AS pickup_zone_id,
            date_trunc('day', pickup_datetime) AS pickup_day,
            service_type,
            COUNT(1) AS total_trips,
            CAST(SUM(total_amount) AS DECIMAL(10,2)) AS daily_revenue,
            CAST(AVG(trip_distance) AS DECIMAL(10,2)) AS avg_distance
        FROM trips_data
        GROUP BY 1, 2, 3
    """)

    """
    =================================================================================
    РАЗДЕЛ 5: ЗАПИСЬ В BIGQUERY (Синергия сервисов)
    =================================================================================
    Здесь происходит магия. Мы пишем данные напрямую в хранилище (DWH).
    
    Внимание на опцию 'temporaryGcsBucket':
    Spark не умеет писать напрямую в таблицы BigQuery по одной строчке (это убило бы сеть).
    Вместо этого коннектор сначала выгружает результат во временные Parquet-файлы 
    в твой GCS бакет, а потом дает команду BigQuery "засосать" эти файлы разом (Load Job).
    Поэтому нам обязательно нужен бакет-посредник.
    """
    # Достаем имя бакета из входящего пути (например: gs://my-bucket/data -> my-bucket)
    temp_bucket = input_path.split("/")[2]

    print(f"💾 Начинаем запись в BigQuery: {bq_table}. Временный бакет: {temp_bucket}")

    # Запись агрегированных данных (Gold Layer)
    df_result.write.format("bigquery").option("table", bq_table).option(
        "temporaryGcsBucket", temp_bucket
    ).mode("overwrite").save()
    # В режиме "overwrite" таблица будет пересоздана. Для продакшена обычно используют "append".

    print("✅ Успех! Оркестратор может гасить свет.")

    # Завершаем сессию. Контейнеры Dataproc Serverless получат сигнал на самоуничтожение.
    spark.stop()


if __name__ == "__main__":
    """
    Точка входа (Entrypoint).
    Эти аргументы передаются из твоего Terraform-файла (блок pyspark_batch -> args).
    """
    parser = argparse.ArgumentParser(description="Dataproc Serverless ETL Taxi")
    parser.add_argument(
        "--input_path", required=True, help="URI до сырых parquet файлов в GCS"
    )
    parser.add_argument(
        "--bq_table",
        required=True,
        help="Целевая таблица в BigQuery (project.dataset.table)",
    )

    args = parser.parse_args()

    # Передаем управление бизнес-логике
    run_etl(args.input_path, args.bq_table)
