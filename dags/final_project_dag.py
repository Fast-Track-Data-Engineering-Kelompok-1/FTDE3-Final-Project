from airflow import DAG
from pendulum import datetime
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from modules.csv_to_db import load_to_postgres_management_payroll, load_to_postgres_performance_management, load_to_mysql_training_development
from modules.csv_to_kafka import load_to_kafka_recruitment_selection
from modules.db_to_postgres_dwh import transfer_postgres_schema_to_another_schema, transfer_mysql_schema_to_postgres, transfer_mongodb_collections_to_postgres
from modules.dbt_transform_to_dwh import profile_config, execution_config, DBT_PROJECT_PATH
from cosmos import DbtTaskGroup, ProjectConfig
from modules.load_to_datamart import extract_transform_spark, load_to_marts

with DAG(
    dag_id="final_project_dag",
    schedule=None,
    start_date=datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["dbt", "postgres", "mysql", "mongodb", "kafka"],
) as dag:
    # Group untuk Dump Data ke Database
    with TaskGroup("dump_data_sql") as tg_load_data:
        task(load_to_postgres_management_payroll)(target_schema_name="kelompok1_db")
        task(load_to_postgres_performance_management)(target_schema_name="kelompok1_db")
        task(load_to_mysql_training_development)(target_schema_name="ftde03")
        task(load_to_kafka_recruitment_selection)(topic_name="ftde03-datamates")
    
    # Group untuk Transfer Data ke Data Warehouse
    with TaskGroup("transfer_data_to_dwh") as tg_transfer_db_to_dwh:
        task(transfer_postgres_schema_to_another_schema)(
            source_postgres_conn_id="postgres_default",
            target_postgres_conn_id="postgres_default",
            source_schema_name="kelompok1_db",
            target_schema_name="kelompok1_dwh_source"
        )
        task(transfer_mysql_schema_to_postgres)(
            mysql_conn_id="mysql_default",
            postgres_conn_id="postgres_default",
            source_schema_name="ftde03",
            target_schema_name="kelompok1_dwh_source"
        )
    
    # Group untuk Transformasi Data ke DWH menggunakan DBT
    tg_dbt = DbtTaskGroup(
        group_id="transform_dwh_data_dbt",
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        profile_config=profile_config,
        execution_config=execution_config,
        default_args={"retries": 2},
    )
    
    # Task untuk Load ke Data Mart (menjalankan extract_transform_spark dan load_to_marts)
    with TaskGroup(group_id="load_to_datamart_group") as load_to_datamart_group:
    # Task untuk menjalankan extract_transform_spark
        task_extract_transform_spark = PythonOperator(
            task_id="extract_transform_spark",
            python_callable=extract_transform_spark,
        )

        # Task untuk menjalankan load_to_marts
        task_load_to_marts = PythonOperator(
            task_id="load_to_marts",
            python_callable=load_to_marts,
        )

        # Menentukan urutan, extract_transform_spark dijalankan sebelum load_to_marts
        task_extract_transform_spark >> task_load_to_marts
    
    # Dependencies
    tg_load_data >> tg_transfer_db_to_dwh >> tg_dbt >> load_to_datamart_group
