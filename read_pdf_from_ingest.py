import sys
import subprocess
import importlib.util

# List of required packages
required_packages = [
    "PyMuPDF"
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
from airflow.hooks.base import BaseHook
from datetime import datetime
import os
import logging
import fitz  # PyMuPDF

PDF_FILENAME = "Bilanz03_EU_neg_EK_kontennachweise.pdf"  # Replace with your actual PDF filename

def read_pdf_from_connection():
    # Get the connection
    conn = BaseHook.get_connection("dsv_ingest")
    
    # Extract the path from the connection's 'Extra' field
    path = conn.extra_dejson.get("path")

    if not path:
        raise ValueError("No 'path' defined in connection 'dsv_ingest' (check Extra field)")

    logging.info(f"Path from 'dsv_ingest' connection: {path}")

    # Check if the path exists
    if not os.path.exists(path):
        raise FileNotFoundError(f"Path does not exist: {path}")
        
    pdf_path = os.path.join(path, PDF_FILENAME)
    
    if not os.path.isfile(pdf_path):
        raise FileNotFoundError(f"File not found: {pdf_path}")

    logging.info(f"Path to pdf file: {pdf_path}")
    
    # Read PDF file
    #with open(pdf_path, "rb") as f:
    #        binary_data = f.read()

     # Read PDF content using PyMuPDF
    with fitz.open(pdf_path) as doc:
        text = ""
        for page in doc:
            text += page.get_text()
    
    # Log first 500 characters (to avoid huge logs)
    logging.info(f"✅ PDF Content (first 500 chars):\n{text[:500]}")
    #logging.info("pdf file read succesfully.")

# Define the DAG
with DAG(
    dag_id="read_pdf_from_dsv_ingest",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    description="Reads a PDF file from the dsv_ingest connection and logs content",
    access_control={'All': {'can_read', 'can_edit', 'can_delete'}}
) as dag:

    read_pdf_task = PythonOperator(
        task_id="read_pdf_file",
        python_callable=read_pdf_from_connection
    )

    read_pdf_task
