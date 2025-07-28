from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
import os
import glob
import logging

# Config
CONN_ID = "dsv_ingest"
PATTERN = "*.pdf"

# Get path from fs connection
def get_connection_path():
    conn = BaseHook.get_connection(CONN_ID)
    base_path = conn.extra_dejson.get("path")
    if not base_path:
        raise ValueError(f"No 'path' defined in extra of connection '{CONN_ID}'")
    return base_path

# Sensor function to check for matching files
def pdf_files_exist(**context):
    path = get_connection_path()
    matches = glob.glob(os.path.join(path, PATTERN))
    if matches:
        logging.info(f"Found {len(matches)} file(s): {matches}")
        return True
    logging.info("⏳ No PDF files found yet.")
    return False

# Define DAG
with DAG(
    dag_id="trigger_on_any_pdf_file",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args={
        "retries": 0,
        "retry_delay": timedelta(minutes=1),
    },
    description="Triggers when any *.pdf file appears in the dsv_ingest path"
) as dag:

    wait_for_any_pdf = PythonSensor(
        task_id="wait_for_pdf_files",
        python_callable=pdf_files_exist,
        poke_interval=30,   # check every 30 seconds
        timeout=900,        # fail after 15 minutes
        mode="poke"         # use "reschedule" if running many DAGs
    )

    log_triggered = PythonOperator(
        task_id="log_triggered_files",
        python_callable=lambda: logging.info("PDF file(s) detected, DAG continues.")
    )

    wait_for_any_pdf >> log_triggered
