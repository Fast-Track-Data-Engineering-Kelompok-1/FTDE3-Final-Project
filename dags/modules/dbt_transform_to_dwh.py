from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping
import os

DBT_TARGET_SCHEMA = 'kelompok1_dwh'
DBT_PROJECT_PATH = f"{os.environ['AIRFLOW_HOME']}/dags/modules/dbt"
DBT_EXECUTABLE_PATH = f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt"

profile_config = ProfileConfig(
    profile_name="dbt_db_to_datawarehouse",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="postgres_default",
        profile_args={"schema": DBT_TARGET_SCHEMA},
    ),
)


execution_config = ExecutionConfig(
    dbt_executable_path=DBT_EXECUTABLE_PATH,
)

