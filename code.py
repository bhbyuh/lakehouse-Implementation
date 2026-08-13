# import json
# import random
# from datetime import datetime
# from kafka import KafkaProducer

# producer = KafkaProducer(
#     bootstrap_servers=["broker:9092"],
#     value_serializer=lambda v: json.dumps(v).encode("utf-8")
# )

# countries = ["IN", "US", "DE", "SG"]
# cities = ["Bangalore", "Delhi", "NY", "Berlin", "Singapore"]
# event_types = ["view", "click", "purchase"]
# categories = ["electronics", "fashion", "books"]
# devices = ["mobile", "desktop"]
# referrers = ["/home", "/search", "/ads"]

# for i in range(1_000_000):  # scale up
#     event = {
#         "event": {
#             "id": f"evt_{i}",
#             "type": random.choice(event_types),
#             "time": datetime.utcnow().isoformat(),
#             "metadata": {  # level 2
#                 "source": "web",
#                 "campaign": {  # level 3
#                     "id": f"camp_{random.randint(1, 1000)}",
#                     "medium": random.choice(["email", "social", "ads"]),
#                     "tags": [f"tag_{random.randint(1,5)}" for _ in range(3)]  # level 4 array
#                 }
#             }
#         },
#         "user": {
#             "id": random.randint(1, 10_000_000),
#             "session_id": f"s_{random.randint(1, 1_000_000)}",
#             "device": random.choice(devices),
#             "geo": {  # level 2
#                 "country": random.choice(countries),
#                 "city": random.choice(cities),
#                 "coordinates": {  # level 3
#                     "lat": round(random.uniform(-90, 90), 6),
#                     "lng": round(random.uniform(-180, 180), 6),
#                     "history": [  # level 4 array
#                         {"timestamp": datetime.utcnow().isoformat(), "lat": round(random.uniform(-90, 90), 6), "lng": round(random.uniform(-180, 180), 6)}
#                         for _ in range(3)
#                     ]
#                 }
#             },
#             "preferences": {  # level 2
#                 "categories": random.sample(categories, 2),
#                 "notifications": {  # level 3
#                     "email": random.choice([True, False]),
#                     "sms": random.choice([True, False]),
#                     "push": random.choice([True, False])
#                 }
#             }
#         },
#         "product": {
#             "id": random.randint(1, 1000),
#             "category": random.choice(categories),
#             "price": round(random.uniform(10, 5000), 2),
#             "quantity": random.randint(1, 5),
#             "variants": [  # level 2 array
#                 {  # level 3
#                     "color": random.choice(["red", "blue", "green"]),
#                     "sizes": [random.choice(["S","M","L","XL"]) for _ in range(2)]  # level 4
#                 } for _ in range(2)
#             ]
#         },
#         "page": {
#             "url": f"/product/{random.randint(1, 1000)}",
#             "referrer": random.choice(referrers),
#             "layout": {  # level 2
#                 "sections": ["header", "body", "footer"],
#                 "ads": {  # level 3
#                     "banner": random.choice([True, False]),
#                     "sidebar": random.choice([True, False]),
#                     "history": [random.randint(0, 10) for _ in range(3)]  # level 4
#                 }
#             }
#         },
#         "meta": {
#             "ingest_time": datetime.utcnow().isoformat(),
#             "version": "1.0",
#             "tags": ["test", "benchmark", f"batch_{i%100}"],
#             "source": {  # level 2
#                 "platform": random.choice(["web", "app"]),
#                 "region": random.choice(countries),
#                 "servers": [f"server_{j}" for j in range(3)]  # level 4 array
#             }
#         }
#     }

#     producer.send("json_four_level_topic", event)

# producer.flush()


from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

data = {
    "userId": 123,
    "action": "login",
    "timestamp": "2024-06-01T12:00:00Z"
}

producer.send('user-actions-topic', value=data)
producer.flush()