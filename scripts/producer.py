import json
import random
import time
from datetime import datetime, timedelta
from kafka import KafkaProducer

KAFKA_BOOTSTRAP_SERVERS = 'localhost:29092'
TOPIC = 'user_activity'

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

users = ['user_A', 'user_B', 'user_C', 'user_D']
pages = ['/home', '/products', '/cart', '/checkout']
event_types = ['page_view', 'click', 'session_start', 'session_end']

print('Producing events to Kafka topic:', TOPIC)

while True:
    # 10% chance of late event (3 minutes old)
    if random.random() < 0.1:
        event_time = datetime.utcnow() - timedelta(minutes=3)
    else:
        event_time = datetime.utcnow()

    event = {
        'event_time': event_time.isoformat() + 'Z',
        'user_id': random.choice(users),
        'page_url': random.choice(pages),
        'event_type': random.choice(event_types)
    }

    producer.send(TOPIC, event)
    print(event)

    time.sleep(1)
