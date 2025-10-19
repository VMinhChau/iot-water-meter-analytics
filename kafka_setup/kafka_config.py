from kafka.admin import KafkaAdminClient, NewTopic
from kafka import KafkaConsumer
import json

class KafkaSetup:
    def __init__(self, bootstrap_servers='localhost:9092'):
        self.admin_client = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers,
            client_id='water_meter_admin'
        )
    
    def create_topics(self):
        topics = [
            NewTopic(name='water-meter-readings', num_partitions=3, replication_factor=1),
            NewTopic(name='processed-readings', num_partitions=3, replication_factor=1),
            NewTopic(name='alerts', num_partitions=1, replication_factor=1)
        ]
        
        try:
            self.admin_client.create_topics(topics)
            print("Topics created successfully")
        except Exception as e:
            print(f"Topics may already exist: {e}")
    
    def consume_messages(self, topic='water-meter-readings', max_messages=10):
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers='localhost:9092',
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='latest'
        )
        
        count = 0
        for message in consumer:
            print(f"Received: {message.value}")
            count += 1
            if count >= max_messages:
                break
        
        consumer.close()

if __name__ == "__main__":
    setup = KafkaSetup()
    setup.create_topics()
    print("Kafka setup complete")