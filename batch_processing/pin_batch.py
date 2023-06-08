import json


pin_invoke_url = "https://spbr0nwvvl.execute-api.us-east-1.amazonaws.com/test/topics/0a4e65e909bd.pin"
            pin_payload = json.dumps({
                "records": [
                    {"value" : pin_result}
                ]
            })