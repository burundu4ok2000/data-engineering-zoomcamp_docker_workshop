import os
import urllib.request
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def main():
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName('homework_q6') \
        .getOrCreate()
        
    data_dir = "homework/pq/yellow_tripdata_2025_11"
    
    if os.path.exists(data_dir):
        df = spark.read.parquet(data_dir)
        print("Using repartitioned data from Q2")
    else:
        df = spark.read.parquet("homework/data/yellow_tripdata_2025-11.parquet")
        print("Using original downloaded data")
        
    df.registerTempTable("yellow")
    
    # Download zone lookup data if it doesn't exist
    zones_url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
    zones_file = "homework/data/taxi_zone_lookup.csv"
    os.makedirs(os.path.dirname(zones_file), exist_ok=True)
    
    if not os.path.exists(zones_file):
        print(f"Downloading {zones_url}...")
        urllib.request.urlretrieve(zones_url, zones_file)
        
    # Read zone lookup data
    zones_df = spark.read \
        .option("header", "true") \
        .csv(zones_file)
        
    zones_df.registerTempTable("zones")
    
    least_frequent = spark.sql("""
        SELECT z.Zone, COUNT(1) as pickup_count
        FROM yellow y
        JOIN zones z ON y.PULocationID = z.LocationID
        GROUP BY z.Zone
        ORDER BY pickup_count ASC
        LIMIT 1
    """).collect()[0]
    
    print(f"Least frequent pickup zone: {least_frequent['Zone']} (Count: {least_frequent['pickup_count']})")
    
if __name__ == "__main__":
    main()
