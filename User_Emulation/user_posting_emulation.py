from time import sleep
import random
import sqlalchemy
from sqlalchemy import text
from User_Emulation.post_data import PostData



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
    '''
    A class to represent a user posting emulation

    Attributes
    ----------
    pin_result : json
        pin data from a random row in the pinterest_data table 
    geo_result : json
        geolocation data from a random row in the pinterest_data table
    user_result : json
        user data from a random row in the pinterest_data table 

    Methods
    -------
    pin_post(connection, random_row)
        gets pin data from a random_row in the pinterest_data table
    geo_post(connection, random_row)
        gets geolocation data from a random_row in the pinterest_data table
    user_post(connection, random_row)
        gets user data from a random_row in the pinterest_data table
    '''

    def __init__(self, connection, random_row):
        '''
        Constructs the attributes for the UserPostingEmulation.

        Parameters
        ----------
        connection : 
            forms a connection between the pinterest data table and the API server which the user emulation is running on
        random_row : int
            a number between 1 and 11,000, corresponding to a row in the pinterest_data table
        '''
        self.pin_result = self.pin_post(connection, random_row)
        self.geo_result = self.geo_post(connection, random_row)
        self.user_result = self.user_post(connection, random_row)


    def pin_post(self, connection, random_row):
        '''
        Gets pin data from a random_row in the pinterest_data table

        Parameters
        ----------
        connection : 
            forms a connection between the pinterest data table and the API server which the user emulation is running on
        random_row : int
            a number between 1 and 11,000, corresponding to a row in the pinterest_data table

        Returns
        -------
        pin_result : json
            pin data from a random row in the pinterest_data table 
        '''
        pin_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
        pin_selected_row = connection.execute(pin_string)
            
        for row in pin_selected_row:
            pin_result = dict(row)
        return pin_result

    def geo_post(self, connection, random_row):
        '''
        Gets geolocation data from a random_row in the pinterest_data table

        Parameters
        ----------
        connection : 
            forms a connection between the pinterest data table and the API server which the user emulation is running on
        random_row : int
            a number between 1 and 11,000, corresponding to a row in the pinterest_data table

        Returns
        -------
        geo_result : json
            geolocation data from a random row in the pinterest_data table 
        '''
        geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
        geo_selected_row = connection.execute(geo_string)
        
        for row in geo_selected_row:
            geo_result = dict(row)
        return geo_result

    def user_post(self,connection, random_row):
        '''
        Gets user data from a random_row in the pinterest_data table

        Parameters
        ----------
        connection : 
            forms a connection between the pinterest data table and the API server which the user emulation is running on
        random_row : int
            a number between 1 and 11,000, corresponding to a row in the pinterest_data table

        Returns
        -------
        user_result : json
            user data from a random row in the pinterest_data table 
        '''
        user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
        user_selected_row = connection.execute(user_string)
        
        for row in user_selected_row:
            user_result = dict(row)
        return user_result
    
new_connector = AWSDBConnector()    
pd = PostData()


def run_infinite_post_data_loop():
    '''
    Runs a loop which posts data from the pinterest_data table

    Parameters
    ----------
    None

    Returns
    None
    '''
    while True:
        sleep(random.randrange(0, 2))
        engine = new_connector.create_db_connector()

        with engine.connect() as connection:
            random_row = random.randint(0, 11000)
            upe = UserPostingEmulation(connection, random_row)

            pin_data_structure = {'index': upe.pin_result["index"], 'unique_id': upe.pin_result["unique_id"], 'title': upe.pin_result["title"], 'description': upe.pin_result["description"], 
                    'poster_name': upe.pin_result["poster_name"], 'follower_count': upe.pin_result["follower_count"], 'tag_list': upe.pin_result["tag_list"], 
                    'is_image_or_video': upe.pin_result["is_image_or_video"], 'image_src': upe.pin_result["image_src"], 'downloaded': upe.pin_result["downloaded"], 
                    'save_location': upe.pin_result["save_location"], 'category': upe.pin_result["category"]}
            geo_data_structure = {"ind": upe.geo_result["ind"], "timestamp": str(upe.geo_result["timestamp"]), "latitude": upe.geo_result["latitude"], 
                                "longitude": upe.geo_result["longitude"], "country": upe.geo_result["country"]}
            user_data_structure = {"ind": upe.user_result["ind"], "first_name": str(upe.user_result["first_name"]), "last_name": upe.user_result["last_name"], 
                                    "age": upe.user_result["age"], "date_joined": str(upe.user_result["date_joined"])}    
                  
            upe.pin_post(connection, random_row)
            upe.geo_post(connection, random_row)
            upe.user_post(connection, random_row)

            pd.post_batch_data(pin_data_structure, geo_data_structure, user_data_structure)
            pd.post_streaming_data(pin_data_structure, geo_data_structure, user_data_structure)

if __name__ == "__main__":
    run_infinite_post_data_loop()
    print('Working')
    