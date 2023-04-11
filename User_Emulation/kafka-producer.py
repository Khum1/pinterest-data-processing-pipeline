from kafka import KafkaProducer
from json import dumps

ml_models = [ #testdata
    {
        "Model_name": "ResNet-50",
        "Accuracy": "92.1",
        "Framework_used": "Pytorch"
    },
    {
        "Model_name": "Random Forest",
        "Accuracy": "82.7",
        "Framework_used": "SKLearn"
    }
] 

retail_data = [ #testdata
    {
        "Item": "42 LCD TV",
        "Price": "209.99",
        "Quantity": "1"
    },
    {
        "Item": "Large Sofa",
        "Price": "259.99",
        "Quantity": 2
    }
]

class MLProducer:
    ml_producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        client_id = "ML data producer",
        value_serializer = lambda mlmsg: dumps(mlmsg).encode("ascii")
    )
    for mlmsg in ml_models:
        ml_producer.send(topic = "MLData, value = mlmsg")
    
    ml_producer.flush()

class RetailProducer:
    retail_producer = KafkaProducer(
        bootstrap_servers = "localhost:9092",
        client_id = "ML data producer",
        value_serializer = lambda retailmsg: dumps(retailmsg).encode("ascii")
    )
    for retailmsg in retail_data:
        retail_producer.send(topic = "Retaildata", value = retailmsg)
    
    retail_producer.flush()

