from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.decorators import task
from datetime import datetime, timedelta
import os

# Constants
PDF_DIR = "/mnt/datasources/vast/glfsshare/DSV/data/pdfs"
CHILD_DAG_ID = "dsv_ocr_workflow"

default_args = {
    "owner": "airflow",
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="start_dsv_ocr_workflow",
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    description="DAG that triggers DSV OCR workflow for each PDF file",
) as dag:

    @task
    def list_pdf_files():
        return [
            f for f in os.listdir(PDF_DIR)
            if f.lower().endswith(".pdf") and os.path.isfile(os.path.join(PDF_DIR, f))
        ]

    @task
    def prepare_trigger_configs(filenames):
        return [{"filename": f} for f in filenames]

    trigger_dsv_ocr_workflow = TriggerDagRunOperator.partial(
        task_id="trigger_dsv_ocr_workflow",
        trigger_dag_id=CHILD_DAG_ID,
        wait_for_completion=False,
    ).expand(conf=prepare_trigger_configs(list_pdf_files()))

