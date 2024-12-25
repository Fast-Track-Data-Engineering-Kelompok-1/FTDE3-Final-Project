from airflow.decorators import dag
from pendulum import datetime
from airflow.decorators import dag, task
from modules.csv_to_db import load_to_postgres_management_payroll, load_to_postgres_performance_management, load_to_mysql_training_development

@dag(
    schedule=None,
    start_date=datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example"],
)
def final_project_dag():
    # Define tasks
    task(load_to_postgres_management_payroll)()
    task(load_to_postgres_performance_management)()
    task(load_to_mysql_training_development)()
    

# Instantiate the DAG
final_project_dag()
