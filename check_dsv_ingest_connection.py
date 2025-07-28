from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime
import logging

def log_path_from_fs_connection():
    # Get connection
    conn = BaseHook.get_connection("dsv_ingest")

    # Get path from extra
    path = conn.extra_dejson.get("path")

    if not path:
        raise ValueError("No 'path' defined in connection 'dsv_ingest' (check Extra field)")

    logging.info(f"📁 Path from 'dsv_ingest' connection: {path}")

    files = os.listdir(path)
    logging.info(f"Files in: {path}")
    for f in files:
        logging.info(f" - {f}")

# Define the DAG
with DAG(
    dag_id="log_fs_connection_path",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    description="Logs the path from the fs connection 'dsv_ingest'"
) as dag:

    log_path_task = PythonOperator(
        task_id="log_connection_path",
        python_callable=log_path_from_fs_connection
    )

    log_path_task
