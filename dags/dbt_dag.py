from cosmos import ProjectConfig, ProfileConfig, ExecutionConfig, DbtTaskGroup
from cosmos.profiles import PostgresUserPasswordProfileMapping
from airflow.sdk import Variable
import os
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.smtp.notifications.smtp import SmtpNotifier
from datetime import datetime
from airflow.decorators import task, dag
import logging
import requests


airflow_home = os.environ["AIRFLOW_HOME"]
postgres_conn_id = Variable.get("POSTGRES_CONN_ID")
dbt_schema = Variable.get("DBT_SCHEMA")


t_log = logging.getLogger("airflow.task")


@task(
    multiple_outputs=True,
    on_success_callback=SmtpNotifier(
        to="touyar.wassim2003@gmail.com",
        subject="Data Ingestion Succeeded",
        html_content="""
        <h3>Airflow Task Success</h3>
        <p><b>DAG:</b> {{ ti.dag_id }}</p>
        <p><b>Task:</b> {{ ti.task_id }}</p>
        <p><b>Execution Date:</b> {{ ts }}</p>
        <p><b>Log URL:</b> <a href="{{ ti.log_url }}">{{ ti.log_url }}</a></p>
    """,
    ),
)
def ingest_data():
    url = "https://storage.googleapis.com/emb-prod-bkt-publicdata/public-downloads/monthly_full_release_long_format.csv"
    response = requests.get(url)
    file_dir = os.path.join(airflow_home, "data")
    os.makedirs(file_dir, exist_ok=True)
    file_path = os.path.join(file_dir, "electricity_source.csv")

    if response.status_code == 200:
        try:
            with open(file_path, "wb") as f:
                f.write(response.content)
            t_log.info("Download complete!")
        except Exception as e:
            t_log.error(f"Failed write the csv file: {e})")
            return {"ok": False, "file_path": file_path}
    else:
        t_log.warning(f"Failed to download, status code: {response.status_code}")
        return {"ok": False, "file_path": file_path}
    return {"ok": True, "file_path": file_path}


@task
def copy_data_to_pg(ok, file_path):
    if ok:
        t_log.info("Starting COPY command")
        pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
        table_name = "electricity.raw_monthly_electricity_data"
        pg_hook.run(f"TRUNCATE {table_name};")
        copy_sql = f"""
        COPY {table_name} FROM STDIN WITH CSV HEADER DELIMITER ','
        """
        pg_hook.copy_expert(copy_sql, file_path)
    else:
        t_log.warning("Skipping COPY command")


profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id=postgres_conn_id,
        profile_args={"schema": dbt_schema},
    ),
)


@dag(
    dag_id="dbt_job",
    schedule="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args={"retries": 2},
)
def dbt_job_dag():
    res = ingest_data()
    load_task = copy_data_to_pg(res["ok"], res["file_path"])

    dbt_tasks = DbtTaskGroup(
        group_id="dbt_tasks",
        project_config=ProjectConfig(
            f"{airflow_home}/dbt_electricity",
        ),
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            dbt_executable_path=f"{airflow_home}/dbt_venv/bin/dbt",
        ),
        on_warning_callback=SmtpNotifier(
            to="touyar.wassim2003@gmail.com",
            subject="dbt Job Warning",
            html_content="""
            <h3>dbt Job Warning</h3>
            <p><b>DAG:</b> {{ ti.dag_id }}</p>
            <p><b>Task:</b> {{ ti.task_id }}</p>
            <p><b>Execution Date:</b> {{ ts }}</p>
            <p><b>Log URL:</b> <a href="{{ ti.log_url }}">{{ ti.log_url }}</a></p>
        """,
        ),
    )

    load_task >> dbt_tasks


dbt_job = dbt_job_dag()
