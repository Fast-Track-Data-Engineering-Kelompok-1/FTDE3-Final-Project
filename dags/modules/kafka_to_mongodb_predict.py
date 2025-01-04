import os
import json
import sys
import pandas as pd
from kafka import KafkaConsumer
from pymongo import MongoClient
from airflow.providers.mongo.hooks.mongo import MongoHook

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from modules.model import modelRecruitment

def get_mongodb_collection():
    # hook = MongoHook(mongo_conn_id="mongodb_default")
    # db = hook.get_conn()['ftde3']
    # collection = db["kelompok1_data_recruitment_selection"]

    try:
        server = MongoClient('mongodb://admin:password@34.56.65.122:27017/')
        db = server.admin
        server_status = db.command("ping")
        print("MongoDB connection successful:", server_status)

        databases = server.list_database_names()
        print("Databases:", databases)

        db = server["ftde3"]
        collection = db["kelompok1_data_recruitment_selection"]

    except Exception as e:
        print("An error was occurred:", e)
    return collection

def predict_load_to_mongodb_from_kafka():
    consumer = KafkaConsumer("ftde03-datamates", 
                             bootstrap_servers='34.56.65.122',
                             auto_offset_reset='earliest')
    collection = get_mongodb_collection()
    model_path = os.path.join(os.environ['AIRFLOW_HOME'],"dags","modules","model")

    for msg in consumer: 
        try: 
            print(f"Records = {json.loads(msg.value)}")

            data = json.loads(msg.value)
            data = pd.DataFrame([data])
            data = data.where(pd.notna(data), None)

            data_predict = data[['Gender','Age','Position','Status']].to_dict('index')[0]
            result = modelRecruitment.runModel(data_predict, model_path)
            data['Predict'] = result

            data_dict = data.to_dict('index')[0]

            collection.update_one(
                {'CandidateID': data_dict['CandidateID']}, 
                {'$set': data_dict},                       
                upsert=True)
        except Exception as e:
            print(f"Error processing message: {e}")
