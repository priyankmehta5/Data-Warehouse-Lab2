from __future__ import annotations

from datetime import datetime
import os
import subprocess

from airflow import DAG
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator


PROJECT_DIR = "/opt/airflow/dags"
SQL_DIR = "/opt/airflow/dags/sql"


def get_runtime_env():
    conn = BaseHook.get_connection("snowflake_con")
    extra = conn.extra_dejson or {}

    env = os.environ.copy()
    env.update({
        "SNOWFLAKE_ACCOUNT": extra.get("account", ""),
        "SNOWFLAKE_USER": conn.login,
        "SNOWFLAKE_PASSWORD": conn.password,
        "SNOWFLAKE_WAREHOUSE": Variable.get("sf_warehouse"),
        "SNOWFLAKE_DATABASE": Variable.get("sf_database"),
        "SNOWFLAKE_SCHEMA": Variable.get("sf_schema"),
        "SNOWFLAKE_ROLE": Variable.get("sf_role"),
        "OPEN_METEO_LATITUDE": Variable.get("open_meteo_latitude"),
        "OPEN_METEO_LONGITUDE": Variable.get("open_meteo_longitude"),
        "OPEN_METEO_TIMEZONE": Variable.get("open_meteo_timezone"),
        "LOCATION_NAME": Variable.get("weather_location_name"),
        "LOAD_MODE": Variable.get("load_mode"),
    })
    return env


def get_snowflake_connection():
    import snowflake.connector

    env = get_runtime_env()

    return snowflake.connector.connect(
        account=env["SNOWFLAKE_ACCOUNT"],
        user=env["SNOWFLAKE_USER"],
        password=env["SNOWFLAKE_PASSWORD"],
        warehouse=env["SNOWFLAKE_WAREHOUSE"],
        database=env["SNOWFLAKE_DATABASE"],
        schema=env["SNOWFLAKE_SCHEMA"],
        role=env["SNOWFLAKE_ROLE"],
        autocommit=False,
    )


def run_python_module(module_name: str):
    result = subprocess.run(
        ["python", "-m", module_name],
        cwd=PROJECT_DIR,
        env=get_runtime_env(),
        capture_output=True,
        text=True,
    )

    print("STDOUT:")
    print(result.stdout)

    if result.stderr:
        print("STDERR:")
        print(result.stderr)

    if result.returncode != 0:
        raise RuntimeError(
            f"Module {module_name} failed with exit code {result.returncode}"
        )


def execute_sql_file_in_transaction(sql_file_name: str):
    sql_file_path = os.path.join(SQL_DIR, sql_file_name)

    if not os.path.exists(sql_file_path):
        raise FileNotFoundError(f"SQL file not found: {sql_file_path}")

    with open(sql_file_path, "r", encoding="utf-8") as f:
        sql_text = f.read()

    conn = get_snowflake_connection()
    cur = conn.cursor()

    try:
        print(f"Running SQL file in transaction: {sql_file_path}")
        cur.execute("BEGIN")

        # Split on semicolon for simple multi-statement execution
        # Keep SQL files simple: one statement per block, no procedure bodies.
        statements = [stmt.strip() for stmt in sql_text.split(";") if stmt.strip()]

        for stmt in statements:
            print("Executing statement:")
            print(stmt)
            cur.execute(stmt)

        conn.commit()
        print(f"Transaction committed for {sql_file_name}")

    except Exception as e:
        print(f"Error while executing {sql_file_name}: {e}")
        conn.rollback()
        print(f"Transaction rolled back for {sql_file_name}")
        raise

    finally:
        cur.close()
        conn.close()


def build_weather_final_sql():
    execute_sql_file_in_transaction("weather_final_union.sql")


def validate_tables():
    conn = get_snowflake_connection()
    cur = conn.cursor()

    try:
        cur.execute("SELECT COUNT(*) FROM OPEN_METEO_RAW")
        raw_count = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM WEATHER_OBSERVATION_HOURLY")
        hourly_count = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM WEATHER_DAILY")
        daily_count = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM WEATHER_FINAL")
        final_count = cur.fetchone()[0]

        print(f"OPEN_METEO_RAW count = {raw_count}")
        print(f"WEATHER_OBSERVATION_HOURLY count = {hourly_count}")
        print(f"WEATHER_DAILY count = {daily_count}")
        print(f"WEATHER_FINAL count = {final_count}")

        if raw_count == 0:
            raise ValueError("Validation failed: OPEN_METEO_RAW is empty.")
        if hourly_count == 0:
            raise ValueError("Validation failed: WEATHER_OBSERVATION_HOURLY is empty.")
        if daily_count == 0:
            raise ValueError("Validation failed: WEATHER_DAILY is empty.")
        if final_count == 0:
            raise ValueError("Validation failed: WEATHER_FINAL is empty.")

        cur.execute("""
            SELECT COUNT(*)
            FROM (
                SELECT LOCATION_NAME, INGEST_TS_UTC, COUNT(*) AS cnt
                FROM OPEN_METEO_RAW
                GROUP BY 1, 2
                HAVING COUNT(*) > 1
            )
        """)
        duplicate_count = cur.fetchone()[0]
        print(f"Potential duplicate raw keys = {duplicate_count}")

    finally:
        cur.close()
        conn.close()


with DAG(
    dag_id="lab2_etl_pipeline",
    start_date=datetime(2026, 4, 1),
    schedule="@daily",
    catchup=False,
    tags=["lab2", "weather", "etl", "snowflake"],
) as dag:

    load_open_meteo_raw = PythonOperator(
        task_id="load_open_meteo_raw",
        python_callable=run_python_module,
        op_kwargs={"module_name": "ETL.open_meteo_raw_loader"},
    )

    transform_weather = PythonOperator(
        task_id="transform_weather",
        python_callable=run_python_module,
        op_kwargs={"module_name": "ETL.weather_transform_loader"},
    )

    build_weather_final = PythonOperator(
        task_id="build_weather_final",
        python_callable=build_weather_final_sql,
    )

    validate_weather_tables = PythonOperator(
        task_id="validate_weather_tables",
        python_callable=validate_tables,
    )

    dbt_run = BashOperator(
    task_id="dbt_run",
    bash_command="cd /opt/airflow/dbt && dbt run --profiles-dir ."
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/dbt && dbt test --profiles-dir ."
    )

    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command="cd /opt/airflow/dbt && dbt snapshot --profiles-dir ."
    )

    load_open_meteo_raw >> transform_weather >> build_weather_final >> validate_weather_tables >> dbt_run >> dbt_test >> dbt_snapshot
