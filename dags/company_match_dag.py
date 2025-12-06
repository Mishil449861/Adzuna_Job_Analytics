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

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/etc/google/auth/application_default_credentials.json"

COMPANY_MATCH_FUNCTION = (
    "https://us-east1-ba882-team4-474802.cloudfunctions.net/company_match"
)

# ------------------------------------------------------------------------------
# FUNCTION TRIGGER
# ------------------------------------------------------------------------------

def trigger_company_match(**kwargs):
    """Call Cloud Function to run company name matching."""
    
    logging.info("Calling Cloud Function: company_match")

    try:
        response = requests.post(
            COMPANY_MATCH_FUNCTION,
            json={"run": True},
            timeout=300
        )

        if response.status_code != 200:
            raise AirflowFailException(
                f"Cloud Function error: {response.status_code} – {response.text}"
            )

        logging.info(f"Cloud Function success: {response.text}")

    except Exception as e:
        raise AirflowFailException(f"Request to Cloud Function failed: {str(e)}")


# ------------------------------------------------------------------------------
# DAG
# ------------------------------------------------------------------------------

with DAG(
    dag_id="company_matching_daily",
    description="Runs company matching via Cloud Function daily.",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={
        "owner": "mishil",
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
    },
) as dag:

    run_company_match = PythonOperator(
        task_id="run_company_match_function",
        python_callable=trigger_company_match,
    )

    run_company_match