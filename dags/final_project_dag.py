from airflow import DAG
from pendulum import datetime
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator
from modules.csv_to_db import load_to_postgres_management_payroll, load_to_postgres_performance_management, load_to_mysql_training_development
from modules.dbt_transform_to_dwh import profile_config,execution_config, DBT_PROJECT_PATH
from cosmos import DbtTaskGroup, ProjectConfig

with DAG(
    dag_id="final_project_dag",
    schedule=None,
    start_date=datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["dbt","postgres","mysql","mongodb","kafka"],
) as dag:
    with TaskGroup("load_data_sql") as tg_load_data:
        task(load_to_postgres_management_payroll)()
        task(load_to_postgres_performance_management)()
        task(load_to_mysql_training_development)()
    
    tg_dbt = DbtTaskGroup(
        group_id="transfom_dbt_db_to_datawarehouse",
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        profile_config=profile_config,
        execution_config=execution_config,
        default_args={"retries": 2},
        )
    
    tg_load_data >> tg_dbt
    
