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

# --- SQL DEFINITIONS ---

CREATE_CLUSTER_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS `{GCP_PROJECT_ID}.{BQ_DATASET_NAME}.job_clusters`
(
  job_id STRING,
  cluster_id INT64,
  seniority STRING,
  model_version STRING,
  processed_at TIMESTAMP
)
PARTITION BY DATE(processed_at);
"""

CREATE_REGISTRY_SQL = f"""
CREATE TABLE IF NOT EXISTS `{GCP_PROJECT_ID}.{BQ_DATASET_NAME}.cluster_registry`
(
  cluster_id INT64,
  cluster_name STRING,
  top_terms STRING,
  model_version STRING,
  updated_at TIMESTAMP
);
"""

CREATE_METRICS_SQL = f"""
CREATE TABLE IF NOT EXISTS `{GCP_PROJECT_ID}.{BQ_DATASET_NAME}.model_metrics`
(
  model_version STRING,
  run_date TIMESTAMP,
  silhouette_score FLOAT64,
  method STRING,
  num_clusters INT64,
  num_jobs_trained INT64
);
"""

@dag(
    dag_id="job_clustering_pipeline",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2025, 11, 1, tz="UTC"),
    catchup=False,
    tags=["ml", "clustering", "bigquery"],
)
def job_clustering_pipeline():

    @task
    def setup_schema():
        key_path = f"/usr/local/airflow/include/{NEW_KEY_FILENAME}"
        if not os.path.exists(key_path):
            raise FileNotFoundError(f"Key file missing: {key_path}")

        credentials = service_account.Credentials.from_service_account_file(key_path)
        bq = bigquery.Client(project=GCP_PROJECT_ID, credentials=credentials)

        bq.query(CREATE_CLUSTER_TABLE_SQL).result()
        bq.query(CREATE_REGISTRY_SQL).result()
        bq.query(CREATE_METRICS_SQL).result()

        print("All tables created or already exist.")

    trigger_model_training = HttpOperator(
        task_id="trigger_model_training",
        http_conn_id=HTTP_CONN_ID,
        method="POST",
        endpoint="train-job-cluster",
        log_response=True,
        execution_timeout=pendulum.duration(minutes=15),
        response_check=lambda res: res.status_code == 200,
    )

    setup_schema() >> trigger_model_training

job_clustering_pipeline()