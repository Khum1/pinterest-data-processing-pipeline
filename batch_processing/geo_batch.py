import json
from User_Emulation.user_posting_emulation import UserPostingEmulation
import requests

upe = UserPostingEmulation()

class GeoPost():
    def __init__(self, headers):
        self.headers = headers
        self.geo_invoke_url = "https://spbr0nwvvl.execute-api.us-east-1.amazonaws.com/test/topics/0a4e65e909bd.geo"

    def __create_payload(self):
        geo_payload = json.dumps({
            "records": [
                {
                "value": {"ind": upe.geo_result["ind"], "timestamp": str(upe.geo_result["timestamp"]), "latitude": upe.geo_result["latitude"], "longitude": upe.geo_result["longitude"],
                            "country": upe.geo_result["country"]}
                }
            ]
        })
        return geo_payload

    def send_request(self):
        geo_payload = self.__create_payload()
        geo_response = requests.request("POST", self.geo_invoke_url, headers=self.headers, data= geo_payload)
        print(geo_response.status_code)
