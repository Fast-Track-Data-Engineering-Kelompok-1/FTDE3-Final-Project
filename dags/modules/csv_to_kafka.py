import os
import json
import pandas as pd
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic

def json_serializer(data):
    return json.dumps(data).encode("utf-8")

def clear_and_create_topic(topic_name, bootstrap_servers):
    admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)

    # Delete the topic if it exists
    try:
        admin_client.delete_topics([topic_name])
        print(f"Topic '{topic_name}' deleted.")
    except Exception as e:
        print(f"Failed to delete topic '{topic_name}' (may not exist): {e}")

    # Recreate the topic
    try:
        new_topic = NewTopic(name=topic_name, num_partitions=1, replication_factor=1)
        admin_client.create_topics([new_topic])
        print(f"Topic '{topic_name}' created.")
    except Exception as e:
        print(f"Failed to create topic '{topic_name}': {e}")

def load_to_kafka_recruitment_selection(topic_name:str, **context):
    csv_path = os.path.join(os.environ['AIRFLOW_HOME'],"include","data_recruitment_selection_update.csv")
    bootstrap_servers=['34.56.65.122']
    clear_and_create_topic(topic_name, bootstrap_servers)

    try:
        data = pd.read_csv(csv_path)
        json_data = data.to_dict(orient='records')
        producer = KafkaProducer(bootstrap_servers=bootstrap_servers, value_serializer=json_serializer)

        for data in json_data:
            print(data)
            producer.send(topic_name, data)
    except FileNotFoundError:
        raise ValueError(f"CSV file not found at: {csv_path}") 
    

