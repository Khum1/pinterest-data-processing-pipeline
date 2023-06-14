import json
import requests


class UserPost():
    '''
    A class to process user post data for the user_posting_emulation

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
    __create_batch_payload(user_result):
        creates the payload for the batch data to be sent to AWS MSK
    __create_streaming_payload(user_reslut):
        creates the payload for the streaming data to be sent to AWS Kinesis
    send_batch_request(user_result):
        sends a POST request to the API for the data to be sent to the user data topic
    send_stream_request(user_result):
        sends a PUT request to the API for the data to be sent to the user data stream 
        '''
    
    def __init__(self, headers):
        '''
        Constructs the neccesary attributes for the user_data to be sent

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
        self.user_invoke_url = "https://spbr0nwvvl.execute-api.us-east-1.amazonaws.com/test/topics/0a4e65e909bd.user"
        self.stream_invoke_url = "https://spbr0nwvvl.execute-api.us-east-1.amazonaws.com/test/streams/streaming-0a4e65e909bd-user/record"


    def __create_batch_payload(self, user_result):
        '''
        Creates the payload for the batch data to be sent to AWS MSK

        Parameters
        ----------
        user_result : dict
            user data from the user_posting_emulation
        
        Returns
        -------
        user_payload : json
            configures the data into json format, ready to send to the user topic
        '''
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
        '''
        Creates the payload for the streaming data to be sent to AWS MSK

        Parameters
        ----------
        user_result : dict
            user data from the user_posting_emulation
        
        Returns
        -------
        user_payload : json
            configures the data into json format, ready to send to the user stream
        '''
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
        '''
        Sends a POST request to the API for the data to be sent to the user data topic

        Parameters
        ----------
        user_result : dict
            user data from the user_posting_emulation
        
        Returns
        -------
        None
        '''
        user_payload = self.__create_batch_payload(user_result)
        user_response = requests.request("POST", self.user_invoke_url, headers=self.headers, data= user_payload)
        print(user_response.status_code)

    def send_stream_request(self, user_result):
        '''
        sends a PUT request to the API for the data to be sent to the user data stream 
        Parameters
        ----------
        user_result : dict
            user data from the user_posting_emulation
        
        Returns
        -------
        None
        '''
        user_payload = self.__create_streaming_payload(user_result)
        user_response = requests.request("PUT", self.stream_invoke_url, headers=self.headers, data=user_payload)
        print(f"user:{user_response.status_code}")
