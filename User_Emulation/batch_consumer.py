from kafka import KafkaConsumer
import json

batch_consumer = KafkaConsumer(
    "MLData",
    bootstrap_servers = "localhost:9092",
    value_deserializer = lambda x: json.loads(x.decode("ascii"))
)

for msg in batch_consumer:
    print(msg)