import json
import time
import pandas as pd
from kafka import KafkaProducer

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

from dotenv import load_dotenv
import os

load_dotenv()
server = os.getenv('REDPANDA_HOST', 'localhost') + ':' + os.getenv('REDPANDA_PORT', '9092')

producer = KafkaProducer(
    bootstrap_servers=[server],
    value_serializer=json_serializer
)

# Read the parquet file
file_path = 'green_tripdata_2025-10.parquet'
df = pd.read_parquet(file_path)

# Keep only necessary columns
columns_to_keep = [
    'lpep_pickup_datetime',
    'lpep_dropoff_datetime',
    'PULocationID',
    'DOLocationID',
    'passenger_count',
    'trip_distance',
    'tip_amount',
    'total_amount'
]
df = df[columns_to_keep]

# Convert datetimes to strings
df['lpep_pickup_datetime'] = df['lpep_pickup_datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')
df['lpep_dropoff_datetime'] = df['lpep_dropoff_datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')

topic_name = 'green-trips'

t0 = time.time()

for row in df.itertuples(index=False):
    row_dict = {col: getattr(row, col) for col in row._fields}
    producer.send(topic_name, value=row_dict)

producer.flush()

t1 = time.time()
print(f'took {(t1 - t0):.2f} seconds')
