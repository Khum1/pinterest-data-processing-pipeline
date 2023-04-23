from kafka import KafkaConsumer
import json

streaming_consumer = KafkaConsumer(
    "MLData",
    bootstrap_servers = "localhost:9092",
    value_deserializer = lambda x: json.loads(x.decode("ascii"))
)

for msg in streaming_consumer:
    print(msg)