from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime
import os
#import fitz  # PyMuPDF
import logging

PDF_FILENAME = "Bilanz03_EU_neg_EK_kontennachweise.pdf"  # Replace with your actual PDF filename

def read_pdf_from_connection():
    # Get the connection
    conn = BaseHook.get_connection("dsv_ingest")
    
    # Get base path from connection (assumes conn.host holds the file path)
    base_path = conn.host
    if not base_path:
        raise ValueError("Connection 'dsv_ingest' does not have a valid file path in host field.")
    
    pdf_path = os.path.join(base_path, PDF_FILENAME)
    logging.info(f"📄 Reading PDF file from: {pdf_path}")

    if not os.path.isfile(pdf_path):
        raise FileNotFoundError(f"File not found: {pdf_path}")

    # Read PDF content using PyMuPDF
    #with fitz.open(pdf_path) as doc:
    #    text = ""
    #    for page in doc:
    #        text += page.get_text()
    
    # Log first 500 characters (to avoid huge logs)
    #logging.info(f"✅ PDF Content (first 500 chars):\n{text[:500]}")
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
