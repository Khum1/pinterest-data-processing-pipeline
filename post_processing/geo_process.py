import json
import requests


class GeoPost():
    def __init__(self, headers):
        self.headers = headers
        self.geo_invoke_url = "https://spbr0nwvvl.execute-api.us-east-1.amazonaws.com/test/topics/0a4e65e909bd.geo"

    def __create_payload(self, geo_result):
        geo_payload = json.dumps({
            "records": [
                {
                "value": {"ind": geo_result["ind"], "timestamp": str(geo_result["timestamp"]), "latitude": geo_result["latitude"], "longitude": geo_result["longitude"],
                            "country": geo_result["country"]}
                }
            ]
        })
        return geo_payload

    def send_batch_request(self, geo_result):
        geo_payload = self.__create_payload(geo_result)
        geo_response = requests.request("POST", self.geo_invoke_url, headers=self.headers, data= geo_payload)
        print(geo_response.status_code)
