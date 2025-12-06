# job_cluster_dag.py
import os
import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.functions import CloudFunctionInvokeFunctionOperator

# --- 1. Import Fix: Add plugins folder to sys.path ---
current_dir = os.path.dirname(os.path.abspath(__file__))
plugins_dir = os.path.join(os.path.dirname(current_dir), 'plugins')

if os.path.exists(plugins_dir):
    if plugins_dir not in sys.path:
        sys.path.insert(0, plugins_dir)
else:
    # Fallback for some Airflow environments
    hardcoded_path = "/usr/local/airflow/plugins"
    if hardcoded_path not in sys.path:
        sys.path.insert(0, hardcoded_path)

PROJECT_ID = "ba882-team4-474802"
LOCATION = "us-central1"
FUNCTION_NAME = "train_job_cluster"
GCP_CONN_ID = "gcp_default"

with DAG(
    dag_id="job_clustering_daily",
    # CRON Expression for 7:00 PM Daily
    # Format: Minute (0) Hour (19) Day(*) Month(*) DayOfWeek(*)
    schedule="0 19 * * *", 
    start_date=datetime(2025, 1, 1), # Updated to 2025 to match your other DAGs
    catchup=False,
    tags=["clustering", "cloud-function", "machine-learning"],
    default_args={
        "owner": "airflow",
        "retries": 3,
        "retry_delay": timedelta(minutes=5)
    }
):

    run_clustering = CloudFunctionInvokeFunctionOperator(
        task_id="run_job_clustering_function",
        project_id=PROJECT_ID,
        location=LOCATION,
        function_id=FUNCTION_NAME,
        input_data={},  # Function expects no JSON input, pulls directly from BQ
    )

    run_clustering