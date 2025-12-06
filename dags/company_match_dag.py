# job_cluster_dag.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator

PROJECT_ID = "ba882-team4-474802"
FUNCTION_URL = "https://us-east1-ba882-team4-474802.cloudfunctions.net/train_job_cluster"

with DAG(
    dag_id="company_matching_daily",
    schedule="0 19 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["clustering", "cloud-function", "machine-learning"],
    default_args={
        "owner": "airflow",
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
    },
):

    run_clustering = SimpleHttpOperator(
        task_id="run_job_clustering_function",
        method="POST",
        http_conn_id=None,                  # ← NO AIRFLOW CONNECTION NEEDED
        endpoint=FUNCTION_URL,              # ← Call Cloud Function directly
        data={},                            # Cloud Function doesn't need input
        headers={"Content-Type": "application/json"},
    )

    run_clustering
