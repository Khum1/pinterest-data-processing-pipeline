import json
from User_Emulation.user_posting_emulation import UserPostingEmulation
import requests

upe = UserPostingEmulation()

class UserPost():
    def __init__(self, headers):
        self.headers = headers
        self.user_invoke_url = "https://spbr0nwvvl.execute-api.us-east-1.amazonaws.com/test/topics/0a4e65e909bd.user"

    def __create_payload(self):
        user_payload = json.dumps({
            "records": [
                {
                "value": {"ind": upe.user_result["ind"], "first_name": str(upe.user_result["first_name"]), "last_name": upe.user_result["last_name"], "age": upe.user_result["age"],
                            "date_joined": str(upe.user_result["date_joined"])}
                }
            ]
        })
        return user_payload
        

    def send_request(self):
        user_payload = self.__create_payload()
        user_response = requests.request("POST", self.user_invoke_url, headers=self.headers, data= user_payload)
        print(user_response.status_code)

