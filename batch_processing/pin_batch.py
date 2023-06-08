import json
from User_Emulation.user_posting_emulation import UserPostingEmulation
import requests

upe = UserPostingEmulation()

class PinPost():

    def __init__(self, headers):
        self.headers = headers
        self.pin_invoke_url = "https://spbr0nwvvl.execute-api.us-east-1.amazonaws.com/test/topics/0a4e65e909bd.pin"

    def __create_payload(self):
        pin_payload = json.dumps({
            "records": [
                {"value" : upe.pin_result}
            ]
        })
        return pin_payload

    def send_request(self):
        pin_payload = self.__create_payload()
        pin_response = requests.request("POST", self.pin_invoke_url, headers=self.headers, data=pin_payload)
        print(pin_response.status_code)
