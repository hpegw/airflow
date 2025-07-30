from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}

FILENAME = "Bilanz03_EU_neg_EK_kontennachweise"

with DAG(
    dag_id="start_dsv_ocr_workflow",
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    description="DAG that triggers DSV OCR workflow and passes a filename",
) as dag:

    trigger_"dsv_ocr_workflow" = TriggerDagRunOperator(
        task_id="trigger_"dsv_ocr_workflow"",
        trigger_dag_id=""dsv_ocr_workflow"",  # Must match actual child DAG
        conf={"filename": FILENAME},    # Pass the filename here
        wait_for_completion=False,
    )
