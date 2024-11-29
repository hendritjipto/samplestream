from confluent_kafka import Producer
import json
import time
from dotenv import load_dotenv
import os

# Kafka topics
ACCOUNT_TOPIC = 'account'
ACCOUNT_EX_TOPIC = 'account_ex'
load_dotenv()

# Configuration for Confluent Cloud
conf = {
    'bootstrap.servers': os.getenv("BOOTSTRAPSRV"),
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': os.getenv("USERNAME"),  # API key
    'sasl.password': os.getenv("PASSWORD")  # API secret
}

# Create producer instance
producer = Producer(conf)

# Callback for producer confirmation
def acked(err, msg):
    if err is not None:
        print(f"Failed to deliver message: {err}")
    else:
        print(f"Message produced to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}")

# Batch 1
batch_1 = [
    {"accountId": 1, "name": "David", "phone": 1111111, "timestamp": "2024-11-04T20:00:00.000"},
    {"accountId": 1, "email": "david@mongodb.com", "timestamp": "2024-11-04T20:00:00.200"},
    {"accountId": 2, "name": "Kevin", "phone": 2222222, "timestamp": "2024-11-04T20:00:00.000"},
    {"accountId": 2, "email": "kevin@mongodb.com", "timestamp": "2024-11-04T20:00:00.200"},
]

# Batch 2
batch_2 = [
    {"accountId": 3, "name": "Hendri", "phone": 3333333, "timestamp": "2024-11-04T20:00:00.000"},
    {"accountId": 3, "email": "hendri@mongodb.com", "timestamp": "2024-11-04T20:00:00.100"},
    {"accountId": 4, "name": "ToSlow", "phone": 4444444, "timestamp": "2024-11-04T20:01:00.000"},
]

#batch 3
batch_3 = [
  {"accountId": 4, "email": "toslow@mongodb.com", "timestamp": "2024-11-04T20:01:40.000"}
]

# Batch 4
batch_4 = [
    {"accountId": 5, "name": "John", "phone": 555555, "timestamp": "2024-11-04T20:02:30.000"},
    {"accountId": 5, "email": "john@mongodb.com", "timestamp": "2024-11-04T20:02:35.200"},
    {"accountId": 6, "name": "Travis", "phone": 6666666, "timestamp": "2024-11-04T20:02:30.000"},
    {"accountId": 6, "email": "travis@mongodb.com", "timestamp": "2024-11-04T20:03:01.200"},
]

def send_batch(batch):
    for record in batch:
        if "name" in record:
            topic = ACCOUNT_TOPIC
        elif "email" in record:
            topic = ACCOUNT_EX_TOPIC
        else:
            continue  # Skip if neither phone nor humidity is present
        
        # Convert record to JSON format
        producer.produce(topic, value=json.dumps(record), callback=acked)
        print(f"Sent to {topic}: {record}")
        
    # Wait for all messages in the batch to be sent
    producer.flush()

# Send the first batch
send_batch(batch_1)

# Wait 5 seconds before sending the second batch
time.sleep(5)

# Send the second batch
send_batch(batch_2)

# Wait 5 seconds before sending the third batch
time.sleep(5)

# Send the second batch
send_batch(batch_3)

# Wait 5 seconds before sending the third batch
time.sleep(5)

# Send the second batch
send_batch(batch_4)