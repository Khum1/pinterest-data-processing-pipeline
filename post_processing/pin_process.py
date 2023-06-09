import json
import requests

class PinPost():

    def __init__(self, headers):
        self.headers = headers
        self.batch_invoke_url = "https://spbr0nwvvl.execute-api.us-east-1.amazonaws.com/test/topics/0a4e65e909bd.pin"
        self.stream_invoke_url = "https://spbr0nwvvl.execute-api.us-east-1.amazonaws.com/test/streams/streams/streaming-0a4e65e909bd-pin/record"

    def __create_batch_payload(self, pin_result):
        pin_payload = json.dumps({
            "records": [
                {"value" : pin_result}
            ]
        })
        return pin_payload
    
    def __create_streaming_payload(self, pin_result):
        pin_payload = json.dumps({
        "StreamName": "streaming-0a4e65e909bd-pin",
        "Data": {
                'index': pin_result["index"], 'unique_id': pin_result["unique_id"], 'title': pin_result["title"], 'description': pin_result["description"], 
                 'poster_name': pin_result["poster_name"], 'follower_count': pin_result["follower_count"], 'tag_list': pin_result["tag_list"], 
                 'is_image_or_video': pin_result["index"], 'image_src': pin_result["index"], 'downloaded': pin_result["index"], 
                 'save_location': pin_result["save_location"], 'category': pin_result["category"]
                },
            "PartitionKey": "pin-partition"
            })
        return pin_payload

    def send_batch_request(self, pin_result):
        pin_payload = self.__create_batch_payload(pin_result)
        pin_response = requests.request("POST", self.batch_invoke_url, headers=self.headers, data=pin_payload)
        print(pin_response.status_code)
    
    def send_stream_request(self, pin_result):
        pin_payload = self.__create_streaming_payload(pin_result)
        pin_response = requests.request("PUT", self.stream_invoke_url, headers=self.headers, data=pin_payload)
        print(pin_response.status_code)
