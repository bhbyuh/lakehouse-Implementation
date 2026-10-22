# pip install kafka-python
# Sends a JSON event every second to the Kafka broker running in Docker.
# Uses the EXTERNAL listener (localhost:9094) exposed in docker-compose.yaml.
import json
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer

TOPIC = "events"

producer = KafkaProducer(
    bootstrap_servers="localhost:9094",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

users = ["alice", "bob", "carol"]

while True:
    event = {
        "user": random.choice(users),
        "amount": random.randint(1, 100),
        "ts": datetime.now(timezone.utc).isoformat(),
    }
    producer.send(TOPIC, event)
    print(event)
    time.sleep(1)
