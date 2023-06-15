import json
import requests

class PinPost():
    '''
    A class to process pin post data for the user_posting_emulation

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
    __create_batch_payload(pin_result):
        creates the payload for the batch data to be sent to AWS MSK
    __create_streaming_payload(pin_reslut):
        creates the payload for the streaming data to be sent to AWS Kinesis
    send_batch_request(pin_result):
        sends a POST request to the API for the data to be sent to the pin data topic
    send_stream_request(pin_result):
        sends a PUT request to the API for the data to be sent to the pin data stream 
        '''
    
    def __init__(self, headers):
        '''
        Constructs the neccesary attributes for the pin_data to be sent

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
        self.batch_invoke_url = "https://spbr0nwvvl.execute-api.us-east-1.amazonaws.com/test/topics/0a4e65e909bd.pin"
        self.stream_invoke_url = "https://spbr0nwvvl.execute-api.us-east-1.amazonaws.com/test/streams/streaming-0a4e65e909bd-pin/record"
   
    def __create_streaming_payload(self, pin_result):
        '''
        Creates the payload for the streaming data to be sent to AWS MSK

        Parameters
        ----------
        pin_result : dict
            pin data from the user_posting_emulation
        
        Returns
        -------
        pin_payload : json
            configures the data into json format, ready to send to the pin stream
        '''
        pin_payload = json.dumps({
        "StreamName": "streaming-0a4e65e909bd-pin",
        "Data": {
                'index': pin_result["index"], 'unique_id': pin_result["unique_id"], 'title': pin_result["title"], 'description': pin_result["description"], 
                 'poster_name': pin_result["poster_name"], 'follower_count': pin_result["follower_count"], 'tag_list': pin_result["tag_list"], 
                 'is_image_or_video': pin_result["is_image_or_video"], 'image_src': pin_result["image_src"], 'downloaded': pin_result["downloaded"], 
                 'save_location': pin_result["save_location"], 'category': pin_result["category"]
                },
            "PartitionKey": "pin-partition"
            })
        return pin_payload

    def send_batch_request(self, pin_result):
        '''
        Sends a POST request to the API for the data to be sent to the pin data topic

        Parameters
        ----------
        pin_result : dict
            pin data from the user_posting_emulation
        
        Returns
        -------
        None
        '''
        pin_payload = json.dumps({"records": [{"value" : pin_result}]})
        pin_response = requests.request("POST", self.batch_invoke_url, headers=self.headers, data=pin_payload)
        print(f"Batch pin: {pin_response.status_code}")
    
    def send_stream_request(self, pin_result):
        '''
        sends a PUT request to the API for the data to be sent to the pin data stream 
        Parameters
        ----------
        pin_result : dict
            pin data from the user_posting_emulation
        
        Returns
        -------
        None
        '''
        pin_payload = self.__create_streaming_payload(pin_result)
        pin_response = requests.request("PUT", self.stream_invoke_url, headers=self.headers, data=pin_payload)
        print(f"Stream pin:{pin_response.status_code}")
