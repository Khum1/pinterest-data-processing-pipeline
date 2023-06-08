from time import sleep
import random
import sqlalchemy
from sqlalchemy import text
import sys
import os
current_directory = os.path.dirname(os.path.abspath(__file__))
parent_directory = os.path.dirname(current_directory)
sys.path.insert(0, parent_directory)
from batch_processing.post_batch_data import PostBatchData



random.seed(100)


class AWSDBConnector:

    def __init__(self):

        self.HOST = "pinterestdbreadonly.cq2e8zno855e.eu-west-1.rds.amazonaws.com"
        self.USER = 'project_user'
        self.PASSWORD = ':t%;yCY3Yjg'
        self.DATABASE = 'pinterest_data'
        self.PORT = 3306
        
    def create_db_connector(self):
        engine = sqlalchemy.create_engine(f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4")
        return engine

class UserPostingEmulation():

    def __init__(self):
        self.random_row = random.randint(0, 11000)

    def pin_post(self, connection):
        pin_string = text(f"SELECT * FROM pinterest_data LIMIT {self.random_row}, 1")
        pin_selected_row = connection.execute(pin_string)
            
        for row in pin_selected_row:
            pin_result = dict(row._mapping)
        self.pin_result = pin_result

    def geo_post(self, connection):
        geo_string = text(f"SELECT * FROM geolocation_data LIMIT {self.random_row}, 1")
        geo_selected_row = connection.execute(geo_string)
        
        for row in geo_selected_row:
            geo_result = dict(row._mapping)
        self.geo_result = geo_result

    def user_post(self,connection):
        user_string = text(f"SELECT * FROM user_data LIMIT {self.random_row}, 1")
        user_selected_row = connection.execute(user_string)
        
        for row in user_selected_row:
            user_result = dict(row._mapping)
        self.user_result = user_result
    
new_connector = AWSDBConnector()    
upe = UserPostingEmulation()
pbd = PostBatchData()


def run_infinite_post_data_loop():
    while True:
        sleep(random.randrange(0, 2))
        engine = new_connector.create_db_connector()

        with engine.connect() as connection:

            upe.pin_post(connection)
            upe.geo_post(connection)
            upe.user_post(connection)

            pbd.post_batch_data(pin_result = upe.pin_result, geo_result = upe.geo_result, user_result = upe.user_result)


if __name__ == "__main__":
    run_infinite_post_data_loop()
    print('Working')
    