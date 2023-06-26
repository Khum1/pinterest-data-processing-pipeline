import json
import requests

class PostData():
    '''
    A class to post data to batch and streaming AWS services

    Attributes
    ----------
    batch_headers : dict
        HTTP headers needed for the API to process the batch request
    streaming_headers : dict
         HTTP headers needed for the API to process the streaming request

    Methods
    -------
    __create_batch_payload(data_structure):
        creates the payload for the batch data to be sent to AWS MSK
    __create_streaming_payload(data_structure, type_of_record):
        creates the payload for the streaming data to be sent to AWS Kinesis
    post_batch_data(pin_result, geo_result, user_result):
        sends batch data requests to the API to be sent to their topics
    
    post_streaming_data(pin_result, geo_result, user_result):
            sends streaming data requests to the API to be sent to their streams

    '''
    def __init__(self):
        self.batch_headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}
        self.streaming_headers = {'Content-Type': 'application/json'}

    def __create_batch_payload(self, data_structure):
        '''
        Creates the payload for the batch data to be sent to AWS MSK

        Parameters
        ----------
        data_structure : dict
            data structure from the result given by user_posting_emulation
        
        Returns
        -------
        payload : json
            configures the data into json format, ready to send to the topic
        '''
        payload = json.dumps({
            "records": [
                {
                "value": data_structure
                }
            ]
        })
        return payload
    
    def __create_streaming_payload(self, data_structure, type_of_record):
        '''
        Creates the payload for the streaming data to be sent to AWS MSK

        Parameters
        ----------
        data_structure : dict
            data structure from the result given by user_posting_emulation
        type_of_record : str
            the type of record that is being added to the df e.g. "pin"
        
        Returns
        -------
        payload : json
            configures the data into json format, ready to send to the stream
        '''
        payload = json.dumps({
        "StreamName": f"streaming-0a4e65e909bd-{type_of_record}",
        "Data": data_structure,
            "PartitionKey": f"{type_of_record}-partition"
            })
        return payload

    def send_batch_request(self, data_structure, type_of_record):
        '''
        Sends a POST request to the API for the data to be sent to the topic

        Parameters
        ----------
        data_structure : dict
            data structure from the result given by user_posting_emulation
        type_of_record : str
            the type of record that is being added to the df e.g. "pin"
        
        Returns
        -------
        None
        '''
        invoke_url = f"https://spbr0nwvvl.execute-api.us-east-1.amazonaws.com/test/topics/0a4e65e909bd.{type_of_record}"
        payload = self.__create_batch_payload(data_structure)
        response = requests.request("POST", invoke_url, headers=self.batch_headers, data= payload)
        print(f"Batch {type_of_record}: {response.status_code}")

    def send_stream_request(self, data_structure, type_of_record):
        '''
        Sends a PUT request to the API for the data to be sent to the stream

        Parameters
        ----------
        data_structure : dict
            data structure from the result given by user_posting_emulation
        type_of_record : str
            the type of record that is being added to the df e.g. "pin"
        
        Returns
        -------
        None
        '''
        invoke_url = f"https://spbr0nwvvl.execute-api.us-east-1.amazonaws.com/test/streams/streaming-0a4e65e909bd-{type_of_record}/record"
        payload = self.__create_streaming_payload(data_structure, type_of_record)
        response = requests.request("PUT", invoke_url, headers=self.streaming_headers, data=payload)
        print(f"Stream {type_of_record}:{response.status_code}")


    def post_batch_data(self, pin_data_structure, geo_data_structure, user_data_structure):
        '''
        Sends batch data requests to the API to be sent to their topics

        Parameters
        ----------
        pin_data_structure : dict
            data structure from the pin_result given by user_posting_emulation
        geo_data_structure : dict
            data structure from the geo_result given by user_posting_emulation
        user_data_structure : dict
            data structure from the user_result given by user_posting_emulation
        
        Returns
        -------
        None
        '''
        self.send_batch_request(geo_data_structure, "geo")
        self.send_batch_request(pin_data_structure, "pin")
        self.send_batch_request(user_data_structure, "user")

    def post_streaming_data(self, pin_data_structure, geo_data_structure, user_data_structure):
        '''
        Sends streaming data requests to the API to be sent to their streams

        Parameters
        ----------
        pin_data_structure : dict
            data structure from the pin_result given by user_posting_emulation
        geo_data_structure : dict
            data structure from the geo_result given by user_posting_emulation
        user_data_structure : dict
            data structure from the user_result given by user_posting_emulation
        
        Returns
        -------
        None
        '''
        self.send_stream_request(pin_data_structure, "pin")
        self.send_stream_request(geo_data_structure, "geo")
        self.send_stream_request(user_data_structure, "user")




