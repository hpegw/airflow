import sys
import subprocess
import importlib.util

# List of required packages
required_packages = [
    "pypdfium2",
    "openai"
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
# imports
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
import logging
from openai import OpenAI
import base64
from pathlib import Path
import shutil
from multiprocessing import Pool
import pypdfium2 as pdfium
import os

# Settings
CONN_ID = "dsv_ingest"
PDF_FILENAME = "Bilanz03_EU_neg_EK_kontennachweise.pdf"
BASE_PATH="/mnt/datasources/vast/glfsshare/DSV/"
# Define the path for ingest data
INGEST_DIR = "data/ingest/"
# Define the path for jpgs
JPG_DIR = "data/img/"
# Define the path for txts
TXT_DIR = "data/txt/"
#Define the path for txts with tables
TABLE_DIR = "data/txt/tablesonly/"
# Define the path for txts with no table
NOTABLES_DIR = "data/txt/notables/"
# Define the path for merged txts
MERGEDTABLES_DIR = "data/txt/tablesonly/merged_tables/"
# Define the directory to scan for PDFs
PDF_DIR = "data/pdfs"
# Define the .json directories base path
JSON_DIR = "data/json"

# Get directory from fs connection
def get_base_path():
    conn = BaseHook.get_connection(CONN_ID)
    path = conn.extra_dejson.get("path")
    if not path
        raise ValueError(f"Connection '{CONN_ID}' has no 'path' in extra.")
    logging.info(f"Path from '{CONN_ID}' connection: {path}")
    return path

def read_pdf_from_connection():
    path = os.path.join(BASE_PATH, INGEST_DIR)
    #logging.info(f"Path from '{CONN_ID}' connection: {path}")
    logging.info(f"Path to file: {path}")

    # Check if the path exists
    if not os.path.exists(path):
        raise FileNotFoundError(f"Path does not exist: {path}")
        
    pdf_path = os.path.join(path, PDF_FILENAME)
    
    if not os.path.isfile(pdf_path):
        raise FileNotFoundError(f"File not found: {pdf_path}")

    logging.info(f"Path to pdf file: {pdf_path}")
    
    # Read PDF file
    with open(pdf_path, "rb") as f:
            binary_data = f.read()
    
    logging.info("pdf file read succesfully.")

# Define DAG
with DAG(
    dag_id="dsv_ocr_workflow",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,  # Manual or sensor-based
    catchup=False,
    default_args={"retries": 0, "retry_delay": timedelta(minutes=1)},
    description="Run the OCR workflow."
) as dag:

    read_pdf_task = PythonOperator(
        task_id="read_pdf_file",
        python_callable=read_pdf_from_connection
    )

    read_pdf_task
