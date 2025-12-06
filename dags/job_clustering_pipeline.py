# job_cluster_dag.py
from datetime import datetime, timedelta
from airflow import DAG
# Note: Ensure you have the google provider installed: pip install apache-airflow-providers-google
from airflow.providers.google.cloud.operators.functions import CloudFunctionInvokeFunctionOperator

PROJECT_ID = "ba882-team4-474802"
LOCATION = "us-central1"
FUNCTION_NAME = "train_job_cluster"

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