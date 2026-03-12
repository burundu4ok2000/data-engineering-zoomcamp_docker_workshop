import pyspark
from pyspark.sql import SparkSession

def main():
    print(f"PySpark version: {pyspark.__version__}")
    
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName('homework') \
        .getOrCreate()
        
    print(f"Spark version: {spark.version}")

if __name__ == "__main__":
    main()
