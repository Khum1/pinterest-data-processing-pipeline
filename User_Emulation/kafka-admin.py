from kafka import KafkaAdminClient
from kafka.admin import NewTopic
from kafka.cluster import ClusterMetadata


admin_client = KafkaAdminClient( # create Kafka client to adminstrate the broker
    bootstrap_servers="localhost:9092", 
    client_id="Kafka Administrator"
)

# topics must be a list to the create_topics method
topics = []
topics.append(NewTopic(name="MLdata", num_partitions=3, replication_factor=1))
topics.append(NewTopic(name="Retaildata", num_partitions=2, replication_factor=1))

admin_client.create_topics(new_topics=topics)