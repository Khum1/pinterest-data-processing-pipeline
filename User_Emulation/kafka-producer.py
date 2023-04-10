from kafka import KafkaProducer
from json import dumps

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer = lambda x: dumps(x).encode("utf-8")
)

json_data = {"User ID": "AWGF76KYU", "Event": "webpage.open"}

producer.send("kafkaPythonTest", json_data)
producer.flush()