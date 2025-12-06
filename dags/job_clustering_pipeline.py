# FILE: dags/job_clustering_dag.py

from datetime import datetime, timedelta
import json
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowFailException
import logging
import os

# -------------------------------------------------------------------------------------
# CONFIG
# -------------------------------------------------------------------------------------

# Use Composer's built-in service account (recommended)
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/etc/google/auth/application_default_credentials.json"

# Cloud Function URLs (UPDATE with your links)
TRAIN_CLUSTER_CF = "https://us-central1-ba882-team4-474802.cloudfunctions.net/train-job-cluster"
SCORE_CLUSTER_CF = "https://us-central1-ba882-team4-474802.cloudfunctions.net/score-job-cluster"

# -------------------------------------------------------------------------------------
# CLOUD FUNCTION CALLERS
# -------------------------------------------------------------------------------------

def trigger_train_cluster(**kwargs):
    """Calls the Cloud Function that trains clusters and writes results to BigQuery."""
    payload = {"message": "run_training"}

    logging.info("Triggering TRAIN cluster Cloud Function...")

    try:
        response = requests.post(TRAIN_CLUSTER_CF, json=payload, timeout=300)

        if response.status_code != 200:
            raise AirflowFailException(
                f"Training CF failed: {response.status_code} – {response.text}"
            )

        logging.info(f"Training CF success: {response.text}")
    except Exception as e:
        raise AirflowFailException(f"Training Cloud Function error: {str(e)}")


def trigger_score_cluster(**kwargs):
    """Calls the Cloud Function that scores new jobs using stored clusters."""
    payload = {"message": "run_scoring"}

    logging.info("Triggering SCORE cluster Cloud Function...")

    try:
        response = requests.post(SCORE_CLUSTER_CF, json=payload, timeout=300)

        if response.status_code != 200:
            raise AirflowFailException(
                f"Scoring CF failed: {response.status_code} – {response.text}"
            )

        logging.info(f"Scoring CF success: {response.text}")
    except Exception as e:
        raise AirflowFailException(f"Scoring Cloud Function error: {str(e)}")


# -------------------------------------------------------------------------------------
# DAG DEFINITION
# -------------------------------------------------------------------------------------

with DAG(
    dag_id="job_clustering_pipeline",
    description="Triggers Cloud Functions to train and score job clusters.",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    max_threads=1,
    default_args={
        "owner": "mishil",
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
    },
) as dag:

    train_cluster_task = PythonOperator(
        task_id="train_job_clusters",
        python_callable=trigger_train_cluster,
    )

    score_cluster_task = PythonOperator(
        task_id="score_job_clusters",
        python_callable=trigger_score_cluster,
    )

    # ----- PIPELINE -----
    train_cluster_task >> score_cluster_task