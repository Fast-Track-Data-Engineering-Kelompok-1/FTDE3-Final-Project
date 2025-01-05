import os
import json
import sys
import warnings
import pandas as pd
from kafka import KafkaConsumer
from pymongo import MongoClient
from airflow.providers.mongo.hooks.mongo import MongoHook
import random
import pymongo
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from modules.model import modelRecruitment

warnings.filterwarnings('ignore')

def get_mongodb_collection():
    hook = MongoHook(mongo_conn_id="mongodb_default")
    connection = hook.get_connection("mongodb_default")

    hostname = connection.host
    port = connection.port
    username = connection.login
    password = connection.password


    try:
        server = MongoClient(f'mongodb://{username}:{password}@{hostname}:{port}/')
        #server = MongoClient('mongodb://admin:password@34.56.65.122:27017/')
        db = server.admin
        server_status = db.command("ping")
        print("MongoDB connection successful:", server_status)

        databases = server.list_database_names()
        print("Databases:", databases)

        db = server["ftde3"]
        collection = db["kelompok1_recruitment_selection"]

    except Exception as e:
        print("An error was occurred:", e)
    return collection

def predict_load_to_mongodb_from_kafka():
    consumer = KafkaConsumer("ftde03-datamates", 
                             bootstrap_servers='34.56.65.122',
                             auto_offset_reset='earliest')
    collection = get_mongodb_collection()
    model_path = os.path.join(os.environ['AIRFLOW_HOME'],"dags","modules","model")
    bulk_updates = []

    counter =0
    for msg in consumer:
        counter +=1
        try: 
            print(f"Records = {json.loads(msg.value)}")

            data = json.loads(msg.value)
            data = pd.DataFrame([data])
            data = data.where(pd.notna(data), None)
            data = data.drop_duplicates(subset=['CandidateID'])

            data_predict = data[['Gender','Age','Position','Status']].to_dict('index')[0]
            result = modelRecruitment.runModel(data_predict, model_path)
            data['Predict'] = result
            #data['Predict'] = data.apply(lambda x : random.choice(["Hired","Not Hired"]),axis=1)

            data_dict = data.to_dict('index')[0]

            bulk_updates.append(
                pymongo.UpdateOne(
                    {'CandidateID': data_dict['CandidateID']}, 
                    {'$set': data_dict}, 
                    upsert=True
                )
            )
        except Exception as e:
            print(f"Error processing message: {e}")
        if counter >=1100:
            break 
    if len(bulk_updates)>0:
        collection.bulk_write(bulk_updates)
