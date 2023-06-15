import json
import requests


class GeoPost():
    '''
    A class to process geolocation post data for the user_posting_emulation

    Attributes
    ----------
    headers :
        HTTP headers needed for the API to process the request
    batch_invoke_url : str
        invoke url to connect to my AWS MSK through my API
    stream_invoke_url : str
        invoke url to connect to AWS Kinesis through my API

    Methods
    -------
    __create_batch_payload(geo_result):
        creates the payload for the batch data to be sent to AWS MSK
    __create_streaming_payload(geo_reslut):
        creates the payload for the streaming data to be sent to AWS Kinesis
    send_batch_request(geo_result):
        sends a POST request to the API for the data to be sent to the geo data topic
    send_stream_request(geo_result):
        sends a PUT request to the API for the data to be sent to the geo data stream 
    '''
    def __init__(self, headers):
        '''
        Constructs the neccesary attributes for the geo_data to be sent

        Parameters
        ----------
        headers :
            HTTP headers needed for the API to process the request
        batch_invoke_url : str
            invoke url to connect to my AWS MSK through my API
        stream_invoke_url : str
            invoke url to connect to AWS Kinesis through my API
        '''
        self.headers = headers
        self.batch_invoke_url = "https://spbr0nwvvl.execute-api.us-east-1.amazonaws.com/test/topics/0a4e65e909bd.geo"
        self.stream_invoke_url = "https://spbr0nwvvl.execute-api.us-east-1.amazonaws.com/test/streams/streaming-0a4e65e909bd-geo/record"


    def __create_batch_payload(self, geo_result):
        '''
        Creates the payload for the batch data to be sent to AWS MSK

        Parameters
        ----------
        geo_result : dict
            geolocation data from the user_posting_emulation
        
        Returns
        -------
        geo_payload : json
            configures the data into json format, ready to send to the geolocation topic
        '''
        geo_payload = json.dumps({
            "records": [
                {
                "value": {"ind": geo_result["ind"], "timestamp": str(geo_result["timestamp"]), "latitude": geo_result["latitude"], "longitude": geo_result["longitude"],
                            "country": geo_result["country"]}
                }
            ]
        })
        return geo_payload
    
    def __create_streaming_payload(self, geo_result):
        '''
        Creates the payload for the streaming data to be sent to AWS MSK

        Parameters
        ----------
        geo_result : dict
            geolocation data from the user_posting_emulation
        
        Returns
        -------
        geo_payload : json
            configures the data into json format, ready to send to the geolocation stream

        '''
        geo_payload = json.dumps({
        "StreamName": "streaming-0a4e65e909bd-geo",
        "Data": {
                "ind": geo_result["ind"], "timestamp": str(geo_result["timestamp"]), "latitude": geo_result["latitude"], "longitude": geo_result["longitude"],
                            "country": geo_result["country"]
                },
            "PartitionKey": "geo-partition"
            })
        return geo_payload

    def send_batch_request(self, geo_result):
        '''
        Sends a POST request to the API for the data to be sent to the geo data topic

        Parameters
        ----------
        geo_result : dict
            geolocation data from the user_posting_emulation
        
        Returns
        -------
        None
        '''
        geo_payload = self.__create_batch_payload(geo_result)
        geo_response = requests.request("POST", self.batch_invoke_url, headers=self.headers, data= geo_payload)
        print(f"Batch geo: {geo_response.status_code}")

    def send_stream_request(self, geo_result):
        '''
        sends a PUT request to the API for the data to be sent to the geo data stream 
        Parameters
        ----------
        geo_result : dict
            geolocation data from the user_posting_emulation
        
        Returns
        -------
        None
        '''
        geo_payload = self.__create_streaming_payload(geo_result)
        pin_response = requests.request("PUT", self.stream_invoke_url, headers=self.headers, data=geo_payload)
        print(f"Stream geo:{pin_response.status_code}")
