# FILE: dags/company_match_dag.py

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

# Use Composer/Astro service account for BigQuery/Cloud Functions authentication
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/etc/google/auth/application_default_credentials.json"

# Your ONLY Cloud Function URL (same one you shared)
JOB_CLUSTER_CF = "https://us-east1-ba882-team4-474802.cloudfunctions.net/train_job_cluster"

# ------------------------------------------------------------------------------
# CLOUD FUNCTION RUNNER
# ------------------------------------------------------------------------------

def call_company_match_cloud_function(**kwargs):
    """
    Calls the cloud function that runs job clustering + company matching_updates.
    """

    logging.info(f"Invoking Cloud Function: {JOB_CLUSTER_CF}")

    try:
        # POST body can be anything; Cloud Function ignores or reads it safely
        response = requests.post(JOB_CLUSTER_CF, json={"trigger": "company_match"}, timeout=300)

        if response.status_code != 200:
            raise AirflowFailException(
                f"Cloud Function returned error {response.status_code}: {response.text}"
            )

        logging.info(f"Cloud Function successful response: {response.text}")

    except Exception as e:
        raise AirflowFailException(
            f"Failed to call Cloud Function: {str(e)}"
        ) from e


# ------------------------------------------------------------------------------
# DAG DEFINITION
# ------------------------------------------------------------------------------

with DAG(
    dag_id="company_matching_daily",
    description="Daily company matching + job clustering pipeline via Cloud Function.",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={
        "owner": "mishil",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["company-matching", "cloud-function", "ml-pipeline"],
) as dag:

    run_company_matching = PythonOperator(
        task_id="run_company_match_function",
        python_callable=call_company_match_cloud_function,
    )

    run_company_matching