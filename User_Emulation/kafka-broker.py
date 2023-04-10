from kafka import KafkaClient
from kafka.cluster import ClusterMetadata

meta_cluster_conn = ClusterMetadata( #create connection to receive metadata
    bootstrap_servers = "localhost:9092", #broker address
)

#retreive metadata
print(meta_cluster_conn.brokers())

#create connection to broker to check its running
client_conn = KafkaClient(
    bootstrap_servers = "localhost:9092",
    client_id = "Broker test" 
)

print(client_conn.bootstrap_connected())
print(client_conn.check_version())