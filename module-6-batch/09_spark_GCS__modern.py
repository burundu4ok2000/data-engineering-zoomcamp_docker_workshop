import marimo

__generated_with = "0.20.4"
app = marimo.App(width="medium")


@app.cell
def _():
    import os

    # Проверяем, что ОС отдала нам переменные
    project_id = os.environ.get("PROJECT_ID")
    bucket_name = os.environ.get("BUCKET_NAME")
    credentials_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")

    if not credentials_path:
        raise ValueError("Переменная GOOGLE_APPLICATION_CREDENTIALS не найдена! Проверь экспорт переменных.")

    print(f"Подключаемся к проекту {project_id}, бакет: {bucket_name}")
    return bucket_name, credentials_path


@app.cell
def _(credentials_path):
    from pyspark.sql import SparkSession

    spark = SparkSession.builder \
        .appName("GCS_Modern_Connection") \
        .config("spark.jars.packages", "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.22") \
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", credentials_path) \
        .getOrCreate()

    # Убираем мусорный вывод логов Info, чтобы не засорять UI Marimo
    spark.sparkContext.setLogLevel("WARN")
    return (spark,)


@app.cell
def _(bucket_name, spark):
    # Формируем путь к данным в облаке. 
    # Замени 'data/*' на реальный путь к файлам внутри твоего бакета, если он отличается.
    gcs_path = f"gs://{bucket_name}/"

    try:
        # Пытаемся прочитать Parquet файлы прямо из Google Cloud
        df_cloud = spark.read.parquet(gcs_path)

        # Выводим схему и первые 5 строк
        df_cloud.printSchema()
        df_cloud.show(5)

    except Exception as e:
        print(f"Ошибка доступа к бакету: {e}")
    return


if __name__ == "__main__":
    app.run()
