from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime
import os
import logging

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
    with open(pdf_path, "rb") as f:
            binary_data = f.read()
    
    logging.info("pdf file read succesfully.")

# Define the DAG
with DAG(
    dag_id="read_pdf_from_dsv_ingest",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    description="Reads a PDF file from the dsv_ingest connection and logs content"
) as dag:

    read_pdf_task = PythonOperator(
        task_id="read_pdf_file",
        python_callable=read_pdf_from_connection
    )

    read_pdf_task
