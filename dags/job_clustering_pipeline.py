# FILE: dags/job_clustering_pipeline.py
#
# This snippet replaces the BigQueryInsertJobOperator with a
# Python task to use your service account key file for auth.

import pendulum
from airflow.decorators import dag, task
from airflow.providers.http.operators.http import HttpOperator

# --- Added imports for key file auth ---
import os
from google.cloud import bigquery
from google.oauth2 import service_account
# ---

# --- Configuration ---
GCP_PROJECT_ID = "ba882-team4-474802"
BQ_DATASET_NAME = "ba882_jobs"
HTTP_CONN_ID = "cloud_function_train_cluster" # You must create this

# This must be the *exact* filename of your new key
NEW_KEY_FILENAME = "ba882-team4-474802-bee53a65f2ac.json"

# This is the SQL from Phase 1
CREATE_CLUSTER_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS `{GCP_PROJECT_ID}.{BQ_DATASET_NAME}.job_clusters`
(
  job_id STRING,
  cluster_id INT64,
  model_version STRING,
  processed_at TIMESTAMP
)
PARTITION BY DATE(processed_at)
OPTIONS (
  description="Stores the K-Means cluster assignment for each job based on its skills."
);
"""

@dag(
    dag_id="job_clustering_pipeline",
    # Runs 3 hours after the skill_extraction_pipeline
    schedule="0 3 */3 * *",
    start_date=pendulum.datetime(2025, 11, 1, tz="UTC"),
    catchup=False,
    tags=["ml", "clustering", "bigquery"],
)
def job_clustering_pipeline():
    """
    DAG to orchestrate the ML model training pipeline.
    1. Ensures the output table exists.
    2. Triggers a Cloud Function to train the K-Means model.
    """

    # --- Task 1: Setup Schema (MODIFIED to use key file) ---
    @task
    def setup_schema():
        """
        Ensures the job_clusters table exists in BigQuery.
        This version authenticates using a service account JSON file.
        """
        print("Ensuring cluster table exists...")
        
        # This is the correct, absolute path *inside the container*
        key_path = f"/usr/local/airflow/include/{NEW_KEY_FILENAME}"

        try:
            credentials = service_account.Credentials.from_service_account_file(key_path)
        except FileNotFoundError:
            print(f"ERROR: Key file not found at {key_path}")
            print("Please ensure the Dockerfile COPY command is correct.")
            raise

        client = bigquery.Client(credentials=credentials, project=GCP_PROJECT_ID)

        try:
            client.query(CREATE_CLUSTER_TABLE_SQL).result()  # Wait for job to complete
            print(f"Successfully ensured table {BQ_DATASET_NAME}.job_clusters exists.")
        except Exception as e:
            print(f"Error executing schema setup query: {e}")
            raise

    # Task 2: Train Model
    # This task calls the HTTP endpoint of your deployed Cloud Function.
    trigger_model_training = HttpOperator(
        task_id="trigger_model_training",
        http_conn_id=HTTP_CONN_ID,
        method="POST",
        endpoint="", # Endpoint is the *path* part of the URL, or empty if URL is in Host
        log_response=True,
    )

    # Define the workflow
    setup_schema_task = setup_schema()  # Call the task function
    setup_schema_task >> trigger_model_training # Set dependency

# Instantiate the DAG
job_clustering_pipeline()