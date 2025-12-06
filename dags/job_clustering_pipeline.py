# FILE: dags/job_clustering_dag.py

from datetime import datetime, timedelta
import requests
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowFailException
import os

# ------------------------------------------------------------------------------
# CONFIG
# ------------------------------------------------------------------------------

# Use Composer's built-in service account for BigQuery auth (recommended)
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/etc/google/auth/application_default_credentials.json"

# Your only Cloud Function
TRAIN_JOB_CLUSTER_CF = "https://us-east1-ba882-team4-474802.cloudfunctions.net/train_job_cluster"

# ------------------------------------------------------------------------------
# CLOUD FUNCTION CALLER
# ------------------------------------------------------------------------------

def trigger_train_job_cluster(**kwargs):
    """Triggers the Cloud Function that trains job clusters and updates BigQuery."""

    logging.info("Calling Cloud Function: train_job_cluster")

    try:
        response = requests.post(TRAIN_JOB_CLUSTER_CF, json={"run": True}, timeout=300)

        if response.status_code != 200:
            raise AirflowFailException(
                f"Cloud Function error: {response.status_code} – {response.text}"
            )

        logging.info(f"Cloud Function success: {response.text}")

    except Exception as e:
        raise AirflowFailException(f"Request to Cloud Function failed: {str(e)}")


# ------------------------------------------------------------------------------
# DAG DEFINITION
# ------------------------------------------------------------------------------

with DAG(
    dag_id="job_clustering_pipeline",
    description="Triggers Cloud Function to train job clusters and update BigQuery.",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={
        "owner": "mishil",
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
    },
) as dag:

    train_job_cluster_task = PythonOperator(
        task_id="train_job_clusters",
        python_callable=trigger_train_job_cluster,
    )

    train_job_cluster_task