import json
import requests


class UserPost():
    def __init__(self, headers):
        self.headers = headers
        self.user_invoke_url = "https://spbr0nwvvl.execute-api.us-east-1.amazonaws.com/test/topics/0a4e65e909bd.user"
        self.stream_invoke_url = "https://spbr0nwvvl.execute-api.us-east-1.amazonaws.com/test/streams/streaming-0a4e65e909bd-user/record"


    def __create_batch_payload(self, user_result):
        user_payload = json.dumps({
            "records": [
                {
                "value": {"ind": user_result["ind"], "first_name": str(user_result["first_name"]), "last_name": user_result["last_name"], "age": user_result["age"],
                            "date_joined": str(user_result["date_joined"])}
                }
            ]
        })
        return user_payload
    
    def __create_streaming_payload(self, user_result):
        user_payload = json.dumps({
        "StreamName": "streaming-0a4e65e909bd-user",
        "Data": {
                "ind": user_result["ind"], "first_name": str(user_result["first_name"]), "last_name": user_result["last_name"], "age": user_result["age"],
                            "date_joined": str(user_result["date_joined"])
                },
            "PartitionKey": "user-partition"
            })
        return user_payload

    def send_batch_request(self, user_result):
        user_payload = self.__create_batch_payload(user_result)
        user_response = requests.request("POST", self.user_invoke_url, headers=self.headers, data= user_payload)
        print(user_response.status_code)

    def send_stream_request(self, user_result):
        user_payload = self.__create_streaming_payload(user_result)
        user_response = requests.request("PUT", self.stream_invoke_url, headers=self.headers, data=user_payload)
        print(f"user:{user_response.status_code}")
