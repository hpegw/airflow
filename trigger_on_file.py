from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
import os
import logging

# Settings
CONN_ID = "dsv_ingest"
FILENAME = "Bilanz03_EU_neg_EK_kontennachweise.pdf"

def log_file_found(**context):
    logging.info(f"✅ File '{FILENAME}' is present and DAG triggered.")

# Get directory from fs connection
def get_base_path():
    conn = BaseHook.get_connection(CONN_ID)
    path = conn.extra_dejson.get("path")
    if not path:
        raise ValueError(f"Connection '{CONN_ID}' has no 'path' in extra.")
    return path

# Define DAG
with DAG(
    dag_id="trigger_on_file_appearance",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,  # Manual or sensor-based
    catchup=False,
    default_args={"retries": 0, "retry_delay": timedelta(minutes=1)},
    description="Triggers when a specific file appears in fs connection path"
) as dag:

    # Sensor: wait for file to appear
    wait_for_file = FileSensor(
        task_id="wait_for_file_trigger",
        filepath=os.path.join(get_base_path(), FILENAME),
        fs_conn_id=CONN_ID,
        poke_interval=30,   # check every 30 seconds
        timeout=600,        # timeout after 10 minutes
        mode="poke"         # or "reschedule" if you prefer
    )

    log_when_triggered = PythonOperator(
        task_id="log_file_found",
        python_callable=log_file_found,
        provide_context=True
    )

    wait_for_file >> log_when_triggered
