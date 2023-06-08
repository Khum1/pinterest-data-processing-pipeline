from .geo_batch import GeoPost
from .pin_batch import PinPost
from .user_batch import UserPost

headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}
geo_post = GeoPost(headers)
pin_post = PinPost(headers)
user_post = UserPost(headers)

class PostBatchData():
    def post_batch_data(self, pin_result, geo_result, user_result):
        geo_post.send_request(geo_result)
        pin_post.send_request(pin_result)
        user_post.send_request(user_result)

