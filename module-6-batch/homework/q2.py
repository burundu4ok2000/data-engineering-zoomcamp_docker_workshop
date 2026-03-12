import os
import urllib.request
from pyspark.sql import SparkSession

def main():
    url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-11.parquet"
    data_dir = "homework/data"
    os.makedirs(data_dir, exist_ok=True)
    file_path = os.path.join(data_dir, "yellow_tripdata_2025-11.parquet")
    
    if not os.path.exists(file_path):
        print(f"Downloading {url} to {file_path}")
        urllib.request.urlretrieve(url, file_path)
        print("Download complete.")
        
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName('homework_q2') \
        .getOrCreate()
        
    print("Reading data...")
    df = spark.read.parquet(file_path)
    
    print("Repartitioning to 4...")
    df = df.repartition(4)
    
    output_dir = "homework/pq/yellow_tripdata_2025_11"
    print(f"Writing to {output_dir}...")
    df.write.parquet(output_dir, mode="overwrite")
    
    print("Calculating average file size...")
    sizes = []
    for f in os.listdir(output_dir):
        if f.endswith(".parquet"):
            sizes.append(os.path.getsize(os.path.join(output_dir, f)))
            
    if sizes:
        avg_size = sum(sizes) / len(sizes)
        print(f"Average size of {len(sizes)} parquet files: {avg_size / (1024 * 1024):.2f} MB")
        print(f"Total size: {sum(sizes) / (1024 * 1024):.2f} MB")
        
if __name__ == "__main__":
    main()
