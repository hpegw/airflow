import sys
import subprocess
import importlib.util


# List of required packages
required_packages = [
    "requests"
]
 
# Check if each package is installed, install if missing
for package in required_packages:
    # Check if the package is already installed
    if importlib.util.find_spec(package) is None:
        print(f"{package} not found, attempting to install...")
        try:
            subprocess.check_call([sys.executable, "-m", "pip", "install", package])
            print(f"Successfully installed {package}")
        except subprocess.CalledProcessError as e:
            print(f"Failed to install {package}: {e}")
            print("Please install the required packages manually and retry.")
            sys.exit(1)

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import requests
import logging

default_args = {
    "owner": "airflow",
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}

def get_google_time():
    try:
        #response = requests.head("https://www.google.com", timeout=5)
        response = requests.head("https://harbor.ezmeral.de", timeout=5)
        date_header = response.headers.get("Date")
        if date_header:
            logging.info(f"Google's server time (from Date header): {date_header}")
        else:
            logging.warning("No Date header received from Google.")
    except Exception as e:
        logging.error(f"Failed to get time from Google: {e}")

with DAG(
    dag_id="log_pip_and_google_time",
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    description="Logs pip list and fetches time from Google",
    access_control={'All': {'can_read', 'can_edit', 'can_delete'}}
) as dag:

    log_pip_list = BashOperator(
        task_id="log_pip_list",
        bash_command="pip list"
    )

    fetch_google_time = PythonOperator(
        task_id="fetch_google_time",
        python_callable=get_google_time
    )

    log_pip_list >> fetch_google_time
