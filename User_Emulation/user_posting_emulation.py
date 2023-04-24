import requests
from time import sleep
import random
from multiprocessing import Process
import boto3
import json
from sqlalchemy import text, create_engine
from kafka import KafkaProducer



random.seed(100)


class AWSDBConnector:

    def __init__(self):

        self.HOST = "pinterestdbreadonly.cq2e8zno855e.eu-west-1.rds.amazonaws.com"
        self.USER = 'project_user'
        self.PASSWORD = ':t%;yCY3Yjg'
        self.DATABASE = 'pinterest_data'
        self.PORT = 3306
        
    def create_db_connector(self):
        engine = create_engine(f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4")
        return engine


new_connector = AWSDBConnector()
kafka_producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        client_id = "ML data producer",
        value_serializer = lambda mlmsg: json.dumps(mlmsg, default = str).encode("ascii")
        )

def run_infinite_post_data_loop():
    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()
        with engine.connect() as connection:
            query = f"SELECT * FROM pinterest_data LIMIT {random_row}, 1"
            selected_row = connection.execute(text(query))
            result = dict(selected_row.mappings().all()[0])
        # for row in selected_row:
        #     print(selected_row)
        #     result = dict(row)
            print(result)
            requests.post("http://localhost:8000/pin/", json=result)
            msg = json.dumps(result, default = str).encode("ascii")
            kafka_producer.send("MLData", value = msg)


if __name__ == "__main__":
    run_infinite_post_data_loop()
    print('Working')
    
    


