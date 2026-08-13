import json
import random
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer


KAFKA_BOOTSTRAP_SERVERS = "localhost:9094"
TOPIC = "events"

EVENTS_PER_SECOND = 5


producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
)


def generate_event():
    return {
        "event_id": str(uuid.uuid4()),
        "customer_id": random.randint(1, 1000),
        "event_type": random.choice(
            ["page_view", "purchase", "login", "logout"]
        ),
        "amount": round(random.uniform(10, 500), 2),
        "event_time": datetime.now(timezone.utc).isoformat(),
    }


def main():
    print(f"Producing events to Kafka topic: {TOPIC}")

    while True:
        event = generate_event()

        producer.send(
            TOPIC,
            key=event["customer_id"].__str__().encode("utf-8"),
            value=event,
        )

        print(event)

        time.sleep(1 / EVENTS_PER_SECOND)


if __name__ == "__main__":
    main()