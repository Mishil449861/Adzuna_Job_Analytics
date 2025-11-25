import pendulum
import os
from airflow.decorators import dag, task
from airflow.providers.http.operators.http import HttpOperator
from google.cloud import bigquery
from google.oauth2 import service_account

# --- Configuration ---
GCP_PROJECT_ID = "ba882-team4-474802"
BQ_DATASET_NAME = "ba882_jobs"
# This ID must match the connection you created in Airflow UI (Admin -> Connections)
HTTP_CONN_ID = "cloud_function_train_cluster" 
# This file must exist inside your Airflow container
NEW_KEY_FILENAME = "ba882-team4-474802-bee53a65f2ac.json"

# --- SQL DEFINITIONS ---

# 1. Job Clusters Table
# Stores the final assignment. 
# Added 'model_version' to track which model created this row.
CREATE_CLUSTER_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS `{GCP_PROJECT_ID}.{BQ_DATASET_NAME}.job_clusters`
(
  job_id STRING,
  cluster_id INT64,
  seniority STRING,       -- Captured from the Python seniority extractor
  model_version STRING,   -- e.g., 'KMeans_v_20251125'
  processed_at TIMESTAMP
)
PARTITION BY DATE(processed_at)
OPTIONS (
  description="Daily cluster assignments. Includes versioning and seniority tags."
);
"""

# 2. Cluster Registry Table
# Stores what "Cluster 1" actually means (e.g., "Senior Data Engineer")
CREATE_REGISTRY_SQL = f"""
CREATE TABLE IF NOT EXISTS `{GCP_PROJECT_ID}.{BQ_DATASET_NAME}.cluster_registry`
(
  cluster_id INT64,
  cluster_name STRING,
  top_terms STRING,
  model_version STRING,
  updated_at TIMESTAMP
)
OPTIONS (
  description="Auto-generated labels for job clusters."
);
"""

# 3. Model Metrics Table
# Stores the 'Tournament' results (Score, Method used, etc.)
CREATE_METRICS_SQL = f"""
CREATE TABLE IF NOT EXISTS `{GCP_PROJECT_ID}.{BQ_DATASET_NAME}.model_metrics`
(
  model_version STRING,
  run_date TIMESTAMP,
  silhouette_score FLOAT64, -- The Quality Score (higher is better)
  method STRING,            -- Which model won? 'KMeans' or 'Hierarchical'
  num_clusters INT64,
  num_jobs_trained INT64
)
OPTIONS (
  description="Tracks the quality and winning method of daily model training."
);
"""

@dag(
    dag_id="job_clustering_pipeline",
    schedule="0 0 * * *", # Runs DAILY at midnight UTC
    start_date=pendulum.datetime(2025, 11, 1, tz="UTC"),
    catchup=False,
    tags=["ml", "clustering", "bigquery", "automl"],
)
def job_clustering_pipeline():
    """
    Orchestrates the ML Pipeline:
    1. Checks/Creates BigQuery tables (Clusters, Registry, Metrics).
    2. Triggers Cloud Function to run the K-Means vs Hierarchical tournament.
    """

    @task
    def setup_schema():
        """
        Ensures the BigQuery infrastructure exists before training starts.
        Uses the Service Account Key file for authentication.
        """
        print("Ensuring ML tables exist...")
        key_path = f"/usr/local/airflow/include/{NEW_KEY_FILENAME}"

        if not os.path.exists(key_path):
            raise FileNotFoundError(f"Critical: Key file missing at {key_path}")

        credentials = service_account.Credentials.from_service_account_file(key_path)
        client = bigquery.Client(credentials=credentials, project=GCP_PROJECT_ID)

        try:
            # 1. Create Main Cluster Table
            client.query(CREATE_CLUSTER_TABLE_SQL).result()
            print("Checked job_clusters table.")
            
            # 2. Create Registry Table
            client.query(CREATE_REGISTRY_SQL).result()
            print("Checked cluster_registry table.")

            # 3. Create Metrics Table
            client.query(CREATE_METRICS_SQL).result()
            print("Checked model_metrics table.")
            
        except Exception as e:
            print(f"Schema setup failed: {e}")
            raise

    # Trigger the Cloud Function
    # The Cloud Function contains the Tournament Logic (K-Means vs Hierarchical)
    trigger_model_training = HttpOperator(
        task_id="trigger_model_training",
        http_conn_id=HTTP_CONN_ID,
        method="POST",
        endpoint="train-job-cluster", 
        log_response=True,
        # TF-IDF + Hierarchical Clustering can be heavy. 
        # Set timeout to 15 minutes to be safe.
        execution_timeout=pendulum.duration(minutes=15), 
        response_check=lambda response: response.status_code == 200,
    )

    setup_schema() >> trigger_model_training

job_clustering_pipeline()