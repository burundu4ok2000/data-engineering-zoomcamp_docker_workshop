import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def main():
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName('homework_q4') \
        .getOrCreate()
        
    data_dir = "homework/pq/yellow_tripdata_2025_11"
    
    if os.path.exists(data_dir):
        df = spark.read.parquet(data_dir)
        print("Using repartitioned data from Q2")
    else:
        df = spark.read.parquet("homework/data/yellow_tripdata_2025-11.parquet")
        print("Using original downloaded data")
        
    df.registerTempTable("yellow")
    
    longest_trip = spark.sql("""
        SELECT MAX(TIMESTAMPDIFF(SECOND, tpep_pickup_datetime, tpep_dropoff_datetime) / 3600.0) as longest_trip_hours
        FROM yellow
    """).collect()[0][0]
    
    print(f"Longest trip (hours): {longest_trip}")
    
if __name__ == "__main__":
    main()
