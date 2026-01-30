import json
import random
import time
from datetime import datetime, timedelta
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:29092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

users = ["user1", "user2", "user3"]
pages = ["/home", "/products", "/cart", "/checkout"]

while True:
    event_time = datetime.utcnow()

    # Occasionally send late event
    if random.random() < 0.1:
        event_time -= timedelta(minutes=3)

    event = {
        "event_time": event_time.isoformat(),
        "user_id": random.choice(users),
        "page_url": random.choice(pages),
        "event_type": random.choice(
            ["page_view", "click", "session_start", "session_end"]
        )
    }

    producer.send("user_activity", event)
    print("Sent:", event)
    time.sleep(1)
