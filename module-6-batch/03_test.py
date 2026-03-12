import marimo

__generated_with = "0.20.4"
app = marimo.App(width="medium")


@app.cell
def _():
    import pyspark

    return (pyspark,)


@app.cell
def _(pyspark):
    pyspark.__file__
    return


@app.cell
def _():
    from pyspark.sql import SparkSession

    return (SparkSession,)


@app.cell
def _(SparkSession):
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName('test') \
        .getOrCreate()
    return (spark,)


@app.cell
def _():
    import os
    import requests
    from pathlib import Path

    def download_file(url: str, destination: str):
        # Создаем папку, если её нет (например, 'data/')
        Path(os.path.dirname(destination)).mkdir(parents=True, exist_ok=True)

        if not os.path.exists(destination):
            print(f"📥 Начинаю загрузку: {url}")
            with requests.get(url, stream=True) as r:
                r.raise_for_status()
                with open(destination, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
            print(f"✅ Файл сохранен: {destination}")
        else:
            print(f"🆗 Файл уже на месте: {destination}")

    # Используем функцию для данных Алексея
    url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
    output = "data/taxi_zone_lookup.csv"

    download_file(url, output)
    return (Path,)


@app.cell
def _():
    # Читаем только первые 5 строк (аналог !head -n 5)
    file_path = "data/taxi_zone_lookup.csv"

    with open(file_path, 'r', encoding='utf-8') as f:
        for _ in range(5):
            print(f.readline().strip())
    return


@app.cell
def _(spark):
    df = spark.read \
        .option("header", "true") \
        .csv('taxi_zone_lookup.csv')
    return (df,)


@app.cell
def _(df):
    df.show()
    return


@app.cell
def _(df):
    df.printSchema()
    return


@app.cell
def _(df):
    df.write.parquet('zones')
    return


@app.cell
def _(Path):
    def list_directory(path_str: str):
        path = Path(path_str)
        if not path.exists():
            print(f"❌ Путь {path_str} не найден")
            return

        print(f"📂 Содержимое: {path_str}/")
        print("-" * 95)

        # Проходим по всем элементам в директории
        for item in path.iterdir():
            # Получаем размер в байтах и переводим в Килобайты
            size_kb = item.stat().st_size / 1024
            # Форматируем строку: выравниваем имя по левому краю, размер — по правому
            print(f"{item.name:<80} | {size_kb:>8.2f} KB")

    # Применяем к нашей новой папке
    list_directory('zones')
    return


@app.cell
def _(spark):
    # Читаем сразу всю папку (Спарк сам найдет внутри .parquet файлы)
    df_parquet = spark.read.parquet('zones')
    return (df_parquet,)


@app.cell
def _(df_parquet):
    # Проверяем схему!
    df_parquet.printSchema()
    return


@app.cell
def _(df_parquet):
    # Смотрим данные
    df_parquet.show(5)
    return


if __name__ == "__main__":
    app.run()
