# job_cluster_dag.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.functions import CloudFunctionInvokeFunctionOperator

PROJECT_ID = "ba882-team4-474802"
LOCATION = "us-central1"
FUNCTION_NAME = "train_job_cluster"

with DAG(
    dag_id="company_matching_daily",
    schedule="0 19 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["clustering", "cloud-function", "machine-learning"],
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
):

    run_clustering = CloudFunctionInvokeFunctionOperator(
        task_id="run_job_clustering_function",
        project_id=PROJECT_ID,
        location=LOCATION,
        function_id=FUNCTION_NAME,
        input_data={},
        gcp_conn_id=None,       # ← THIS FIXES YOUR ERROR
    )

    run_clustering