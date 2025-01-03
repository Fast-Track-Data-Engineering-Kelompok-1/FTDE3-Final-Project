import json
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
from airflow.providers.mongo.hooks.mongo import MongoHook


def get_mongodb_collection():
    hook = MongoHook(mongo_conn_id="mongodb_default")
    db = hook.get_conn()['ftde3']
    collection = db["kelompok1_data_recruitment_selection_raw"]
    return collection

def consume_function(message, name):
    mongodb_collection = get_mongodb_collection()

    key = json.loads(message.key())
    message_content = json.loads(message.value())

# to be continued