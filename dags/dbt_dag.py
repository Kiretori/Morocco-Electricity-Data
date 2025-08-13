from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping
from airflow.sdk import Variable
import os
from datetime import datetime

airflow_home = os.environ["AIRFLOW_HOME"]
postgres_conn_id = Variable.get("POSTGRES_CONN_ID")
dbt_schema = Variable.get("DBT_SCHEMA")


profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id=postgres_conn_id,
        profile_args={"schema": dbt_schema},
    ),
)

my_cosmos_dag = DbtDag(
    project_config=ProjectConfig(
        f"{airflow_home}/dbt_electricity",
    ),
    profile_config=profile_config,
    execution_config=ExecutionConfig(
        dbt_executable_path=f"{airflow_home}/dbt_venv/bin/dbt",
    ),
    schedule="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    dag_id="dbt_job",
    default_args={"retries": 2},
)