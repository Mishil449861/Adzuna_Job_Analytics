import pendulum
import os
from airflow.decorators import dag, task
from airflow.providers.http.operators.http import HttpOperator
from google.cloud import bigquery
from google.oauth2 import service_account

# --- Configuration ---
GCP_PROJECT_ID = "ba882-team4-474802"
BQ_DATASET_NAME = "ba882_jobs"
HTTP_CONN_ID = "cloud_function_train_cluster" 
NEW_KEY_FILENAME = "ba882-team4-474802-bee53a65f2ac.json"

# 1. The Job Clusters Table 
# UPDATE: Changed 'seniority_level' to 'seniority' to match your Python DataFrame
CREATE_CLUSTER_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS `{GCP_PROJECT_ID}.{BQ_DATASET_NAME}.job_clusters`
(
  job_id STRING,
  cluster_id INT64,
  seniority STRING, -- Matches df['seniority'] in main.py
  processed_at TIMESTAMP
)
PARTITION BY DATE(processed_at)
OPTIONS (
  description="Stores the TF-IDF + K-Means cluster assignment and seniority tag for each job."
);
"""

# 2. The Cluster Registry
CREATE_REGISTRY_SQL = f"""
CREATE TABLE IF NOT EXISTS `{GCP_PROJECT_ID}.{BQ_DATASET_NAME}.cluster_registry`
(
  cluster_id INT64,
  cluster_name STRING,
  top_terms STRING,
  updated_at TIMESTAMP
)
OPTIONS (
  description="Auto-generated labels for job clusters (e.g., 'Senior Data Engineer')."
);
"""

@dag(
    dag_id="job_clustering_pipeline",
    schedule="0 0 * * *", # Runs DAILY
    start_date=pendulum.datetime(2025, 11, 1, tz="UTC"),
    catchup=False,
    tags=["ml", "clustering", "bigquery"],
)
def job_clustering_pipeline():
    """
    Orchestrates daily ML retraining.
    1. Checks/Creates BigQuery tables.
    2. Triggers Cloud Function to run TF-IDF & K-Means.
    """

    @task
    def setup_schema():
        """
        Ensures both the cluster assignment table and the label registry exist.
        """
        print("Ensuring ML tables exist...")
        key_path = f"/usr/local/airflow/include/{NEW_KEY_FILENAME}"

        if not os.path.exists(key_path):
            raise FileNotFoundError(f"Critical: Key file missing at {key_path}")

        credentials = service_account.Credentials.from_service_account_file(key_path)
        client = bigquery.Client(credentials=credentials, project=GCP_PROJECT_ID)

        try:
            # Create/Check Main Table
            client.query(CREATE_CLUSTER_TABLE_SQL).result()
            print("Checked job_clusters table.")
            
            # Create/Check Registry Table
            client.query(CREATE_REGISTRY_SQL).result()
            print("Checked cluster_registry table.")
            
        except Exception as e:
            print(f"Schema setup failed: {e}")
            raise

    # Trigger the Cloud Function
    trigger_model_training = HttpOperator(
        task_id="trigger_model_training",
        http_conn_id=HTTP_CONN_ID,
        method="POST",
        endpoint="train-job-cluster", 
        log_response=True,
        # TF-IDF on large datasets can take a moment, so we allow 10 mins
        execution_timeout=pendulum.duration(minutes=10), 
        response_check=lambda response: response.status_code == 200,
    )

    setup_schema() >> trigger_model_training

job_clustering_pipeline()