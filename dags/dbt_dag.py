from cosmos import ProjectConfig, ProfileConfig, ExecutionConfig, DbtTaskGroup
from cosmos.profiles import PostgresUserPasswordProfileMapping
from airflow.sdk import Variable
import os
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
from airflow.decorators import task
import logging
import requests
import pandas as pd 


airflow_home = os.environ["AIRFLOW_HOME"]
postgres_conn_id = Variable.get("POSTGRES_CONN_ID")
dbt_schema = Variable.get("DBT_SCHEMA")


t_log = logging.getLogger("airflow.task")


@task
def ingest_data():
    url = "https://storage.googleapis.com/emb-prod-bkt-publicdata/public-downloads/monthly_full_release_long_format.csv"
    response = requests.get(url)

    file_dir = os.path.join(airflow_home, "data")
    os.makedirs(file_dir, exist_ok=True)

    file_path = os.path.join(file_dir, "electricity_source.csv")
    if response.status_code == 200:
        with open(file_path, "wb") as f:
            f.write(response.content)
        t_log.info("Download complete!")
    else:
        t_log.warning(f"Failed to download: {response.status_code}" )
    
    return file_path

@task
def copy_data_to_pg(file_path):
    pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    table_name = "electricity.raw_monthly_electricity_data"
    
    pg_hook.run(f"TRUNCATE {table_name};")


    copy_sql = f"""
    COPY {table_name} FROM STDIN WITH CSV HEADER DELIMITER ','
    """
    pg_hook.copy_expert(copy_sql, file_path)


profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id=postgres_conn_id,
        profile_args={"schema": dbt_schema},
    ),
)

with DAG(
    dag_id="dbt_job",
    schedule="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args={"retries": 2},
) as dbt_job:
    
    file_path = ingest_data()

    load_task = copy_data_to_pg(file_path)

    dbt_tasks = DbtTaskGroup(
        group_id="dbt_tasks",
        project_config=ProjectConfig(
            f"{airflow_home}/dbt_electricity",
        ),
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            dbt_executable_path=f"{airflow_home}/dbt_venv/bin/dbt",
        ),
    )

    load_task >> dbt_tasks


