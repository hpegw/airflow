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
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="log_installed_python_packages",
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    description="Logs the output of `pip list` to Airflow logs",
) as dag:

    log_pip_list = BashOperator(
        task_id="log_pip_list",
        bash_command="pip list"
    )
