import os
import json
import time
import kafka
import pandas as pd

from kafka import KafkaProducer

def json_serializer(data):
    return json.dumps(data).encode("utf-8")

def load_to_kafka_recruitment_selection(topic_name:str, **context):
    csv_path = os.path.join(os.environ['AIRFLOW_HOME'],"include","data_recruitment_selection_update.csv")

    try:
        data = pd.read_csv(csv_path)
        json_data = data.to_dict(orient='records')
        producer = KafkaProducer(bootstrap_servers=['34.56.65.122'], value_serializer=json_serializer)

        for data in json_data:
            print(data)
            producer.send(topic_name, data)
    except FileNotFoundError:
        raise ValueError(f"CSV file not found at: {csv_path}") 
    

