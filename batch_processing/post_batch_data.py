from geo_batch import GeoPost
from pin_batch import PinPost
from user_batch import UserPost


geo_post = GeoPost()
pin_post = PinPost()
user_post = UserPost()

class PostBatchData():
    def __init__(self):
        self.headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}
        
    def post_batch_data(self):
        geo_post.send_request()
        pin_post.send_request()
        user_post.send_request()

