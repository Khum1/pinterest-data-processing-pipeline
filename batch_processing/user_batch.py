import json
import requests


class UserPost():
    def __init__(self, headers):
        self.headers = headers
        self.user_invoke_url = "https://spbr0nwvvl.execute-api.us-east-1.amazonaws.com/test/topics/0a4e65e909bd.user"

    def __create_payload(self, user_result):
        user_payload = json.dumps({
            "records": [
                {
                "value": {"ind": user_result["ind"], "first_name": str(user_result["first_name"]), "last_name": user_result["last_name"], "age": user_result["age"],
                            "date_joined": str(user_result["date_joined"])}
                }
            ]
        })
        return user_payload
        

    def send_request(self, user_result):
        user_payload = self.__create_payload(user_result)
        user_response = requests.request("POST", self.user_invoke_url, headers=self.headers, data= user_payload)
        print(user_response.status_code)

