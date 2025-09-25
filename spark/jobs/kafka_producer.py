from kafka import KafkaProducer
import json
import time
import random
import uuid

KAFKA_BROKER = "localhost:9094"
KAFKA_TOPIC = "taxi-trips"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

while True:
    message = {
        "trip_id": str(uuid.uuid4()),
        "fare": round(random.uniform(5, 100), 2),
        "distance": round(random.uniform(1, 50), 2),
        "pickup_latitude": round(random.uniform(-90, 90), 6),
        "pickup_longitude": round(random.uniform(-180, 180), 6)
    }
    producer.send(KAFKA_TOPIC, message)
    print(f"Sent: {message}")
    time.sleep(1)
