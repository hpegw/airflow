from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime
import logging
import os

def log_path_and_files():
    # Get the 'dsv_ingest' connection
    conn = BaseHook.get_connection("dsv_ingest")

    # Extract the path from the connection's 'Extra' field
    path = conn.extra_dejson.get("path")

    if not path:
        raise ValueError("No 'path' defined in connection 'dsv_ingest' (check Extra field)")

    logging.info(f"📁 Path from 'dsv_ingest' connection: {path}")

    # Check if the path exists
    if not os.path.exists(path):
        raise FileNotFoundError(f"Path does not exist: {path}")

    # List and log files/directories
    entries = os.listdir(path)
    if not entries:
        logging.info("📂 Directory is empty.")
    else:
        logging.info("📄 Files and subdirectories:")
        for item in entries:
            full_path = os.path.join(path, item)
            if os.path.isfile(full_path):
                logging.info(f" - [File] {item}")
            elif os.path.isdir(full_path):
                logging.info(f" - [Dir ] {item}")
            else:
                logging.info(f" - [Other] {item}")

# Define the DAG
with DAG(
    dag_id="log_fs_connection_path_and_files",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    description="Logs the path and files from the fs connection 'dsv_ingest'"
) as dag:

    log_task = PythonOperator(
        task_id="log_path_and_files",
        python_callable=log_path_and_files
    )

    log_task
