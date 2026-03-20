import json
from kafka import KafkaConsumer

import os
from dotenv import load_dotenv

load_dotenv()
server = os.getenv('REDPANDA_HOST', 'localhost') + ':' + os.getenv('REDPANDA_PORT', '9092')
topic_name = 'green-trips'

consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=[server],
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    consumer_timeout_ms=5000
)

count = 0
for msg in consumer:
    trip_distance = msg.value.get('trip_distance', 0)
    if trip_distance > 5.0:
        count += 1

print(f"Trips with distance > 5.0: {count}")
