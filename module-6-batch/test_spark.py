import os
import sys
from pyspark.sql import SparkSession

# Создаем сессию
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("DellInspironTest") \
    .getOrCreate()

print("\n" + "="*50)
print(f"ПРОЦЕСС ЗАПУЩЕН НА JAVA_HOME: {os.environ.get('JAVA_HOME')}")
print(f"ВЕРСИЯ SPARK: {spark.version}")
# Используем стандартный sys.executable вместо конфига Spark
print(f"ИСПОЛЬЗУЕМЫЙ ПИТОН: {sys.executable}")
print("="*50 + "\n")

# Создаем данные
data = [("Engine", 100), ("Turbine", 200), ("Sensor", 300)]
df = spark.createDataFrame(data, ["Component", "Value"])

# Выполняем действие
df.show()

# Проверяем параллелизм
parallelism = spark.sparkContext.defaultParallelism
print(f"Уровень параллелизма (количество потоков): {parallelism}")

if parallelism >= 8:
    print("\nБОНУС: Spark видит все 8 потоков твоего i5-8250U! Камаз готов к перегрузкам.")

spark.stop()
