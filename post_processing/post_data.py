from .geo_process import GeoPost
from .pin_process import PinPost
from .user_process import UserPost

batch_headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}
geo_batch = GeoPost(batch_headers)
pin_batch = PinPost(batch_headers)
user_batch = UserPost(batch_headers)

streaming_headers = {'Content-Type': 'application/json'}
pin_streaming = PinPost(streaming_headers)

class PostData():
    def post_batch_data(self, pin_result, geo_result, user_result):
        geo_batch.send_batch_request(geo_result)
        pin_batch.send_batch_request(pin_result)
        user_batch.send_batch_request(user_result)

    def post_streaming_data(self, pin_result):
        pin_streaming.send_stream_request(pin_result)


