from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import logging

# Function to log directory info
def log_current_directory_and_files():
    cwd = os.getcwd()
    files = os.listdir(cwd)

    logging.info(f"📁 Current Working Directory: {cwd}")
    logging.info("📄 Files and directories:")
    for f in files:
        logging.info(f" - {f}")

# Define the DAG
with DAG(
    dag_id="log_directory_and_files",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,  # Manual trigger
    catchup=False,
    description="Logs the current working directory and list of files"
) as dag:

    log_task = PythonOperator(
        task_id="log_directory_and_files_task",
        python_callable=log_current_directory_and_files,
    )

    log_task
