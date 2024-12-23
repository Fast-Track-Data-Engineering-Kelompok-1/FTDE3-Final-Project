from airflow import DAG, Dataset
from airflow.decorators import dag, task
from pendulum import datetime
import requests
import pandas as pd
import os
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.mysql.hooks.mysql import MySqlHook
from sqlalchemy.engine import Engine

@dag(
    schedule=None,
    start_date=datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example"],
)
def csv_to_db():
    # Define tasks

    @task
    def load_to_postgres_management_payroll(**context):
        csv_path = os.path.join("/usr/local/airflow","include","data_management_payroll_update.csv")
        try:
            df = pd.read_csv(csv_path)
        except FileNotFoundError:
            raise ValueError(f"CSV file not found at: {csv_path}") 
        postgres_hook = PostgresHook(postgres_conn_id="postgres_default")
        engine:Engine = postgres_hook.get_sqlalchemy_engine()
        table_name = "kelompok1_data_management_payroll"
        df.to_sql(name=table_name,con=engine.connect(),if_exists="replace",index=False)
        print(f"Load to {table_name} successful")

    @task
    def load_to_postgres_performance_management(**context):
        csv_path = os.path.join("/usr/local/airflow","include","data_performance_management_update.csv")
        try:
            df = pd.read_csv(csv_path)
        except FileNotFoundError:
            raise ValueError(f"CSV file not found at: {csv_path}") 
        postgres_hook = PostgresHook(postgres_conn_id="postgres_default")
        engine:Engine = postgres_hook.get_sqlalchemy_engine()
        table_name = "kelompok1_performance_management_payroll"
        df.to_sql(name=table_name,con=engine.connect(),if_exists="replace",index=False)
        print(f"Load to {table_name} successful")

    @task
    def load_to_mysql_training_development(**context):
        csv_path = os.path.join("/usr/local/airflow","include","data_training_development_update.csv")
        try:
            df = pd.read_csv(csv_path)
        except FileNotFoundError:
            raise ValueError(f"CSV file not found at: {csv_path}") 
        postgres_hook = MySqlHook(mysql_conn_id="mysql_default")
        engine:Engine = postgres_hook.get_sqlalchemy_engine()
        table_name = "kelompok1_training_development_payroll"
        df.to_sql(name=table_name,con=engine.connect(),if_exists="replace",index=False)
        print(f"Load to {table_name} successful")
    
    load_to_postgres_management_payroll()
    load_to_postgres_performance_management()
    load_to_mysql_training_development()
    

# Instantiate the DAG
csv_to_db()
