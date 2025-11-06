import os
import logging
from datetime import datetime, timedelta
from google.cloud import secretmanager
from airflow.decorators import dag, task
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
import sys
from pipeline_utils import fetch_data, transform_data, upload_dfs_to_gcs
from google.oauth2 import service_account
from google.cloud import secretmanager

dag_dir = os.path.dirname(os.path.abspath(__file__))
plugins_dir = os.path.join(os.path.dirname(dag_dir), 'plugins')
if plugins_dir not in sys.path:
    sys.path.insert(0, plugins_dir)

# --- Project Configuration ---
GCP_PROJECT_ID = "ba882-team4-474802"
GCS_BUCKET_NAME = "adzuna-bucket"
BQ_DATASET_NAME = "ba882_jobs"
GCP_CONN_ID = "gcp_default"

# --- Adzuna API Configuration ---
SECRET_APP_ID_NAME = "ADZUNA_APP_ID"
SECRET_APP_KEY_NAME = "ADZUNA_APP_KEY"

# --- SQL File Paths ---
SQL_DIR = "/usr/local/airflow/include/sql"
try:
    with open(os.path.join(SQL_DIR, "merge_jobs.sql")) as f:
        merge_jobs_sql = f.read()
    with open(os.path.join(SQL_DIR, "merge_jobstats.sql")) as f:
        merge_jobstats_sql = f.read()
    with open(os.path.join(SQL_DIR, "merge_companies.sql")) as f:
        merge_companies_sql = f.read()
    with open(os.path.join(SQL_DIR, "merge_locations.sql")) as f:
        merge_locations_sql = f.read()
    with open(os.path.join(SQL_DIR, "merge_categories.sql")) as f:
        merge_categories_sql = f.read()
except FileNotFoundError:
    logging.error(f"SQL files not found in {SQL_DIR}.")
    merge_jobs_sql, merge_jobstats_sql, merge_companies_sql, merge_locations_sql, merge_categories_sql = "", "", "", "", ""

# --- DEFAULT ARGUMENTS ---
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
}

# --- Airflow DAG Definition ---
@dag(
    dag_id="adzuna_gcs_to_bigquery_star_schema_v2",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule="0 0 * * *",
    catchup=False,
    tags=["adzuna", "gcs", "bigquery", "star-schema", "merge"],
)
def adzuna_pipeline_merge():

    @task
    def get_adzuna_secrets() -> dict:
        key_path = "/usr/local/airflow/include/ba882-team4-474802-964ab07e73f5.json"
        credentials = service_account.Credentials.from_service_account_file(key_path)
        client = secretmanager.SecretManagerServiceClient(credentials=credentials)

        app_id_name = f"projects/{GCP_PROJECT_ID}/secrets/{SECRET_APP_ID_NAME}/versions/latest"
        app_key_name = f"projects/{GCP_PROJECT_ID}/secrets/{SECRET_APP_KEY_NAME}/versions/latest"

        logging.info("Fetching secrets...")
        app_id = client.access_secret_version(request={"name": app_id_name}).payload.data.decode("UTF-8")
        app_key = client.access_secret_version(request={"name": app_key_name}).payload.data.decode("UTF-8")
        logging.info("✅ Successfully fetched Adzuna secrets.")
        return {"app_id": app_id, "app_key": app_key}

    @task
    def extract_transform_and_load_to_gcs(secrets: dict) -> dict:
        ingest_timestamp = datetime.utcnow()
        app_id = secrets["app_id"]
        app_key = secrets["app_key"]

        records = fetch_data(app_id, app_key)
        if not records:
            raise ValueError("No records fetched from Adzuna API.")

        dfs = transform_data(records, ingest_timestamp)
        gcs_paths = upload_dfs_to_gcs(dfs, GCS_BUCKET_NAME, ingest_timestamp, GCP_CONN_ID)

        if not gcs_paths:
            raise ValueError("No files were uploaded to GCS.")

        return gcs_paths

    # --- Task Group 1: Load GCS to Staging Tables ---
    load_jobs_to_staging = GCSToBigQueryOperator(
        task_id="load_jobs_to_staging",
        bucket=GCS_BUCKET_NAME,
        source_objects=["{{ task_instance.xcom_pull(task_ids='extract_transform_and_load_to_gcs')['jobs'] }}"],
        destination_project_dataset_table=f"{GCP_PROJECT_ID}.{BQ_DATASET_NAME}.staging_jobs",
        source_format="PARQUET",
        write_disposition="WRITE_TRUNCATE", 
        create_disposition="CREATE_IF_NEEDED",
        gcp_conn_id=GCP_CONN_ID,
    )
    load_categories_to_staging = GCSToBigQueryOperator(
        task_id="load_categories_to_staging",
        bucket=GCS_BUCKET_NAME,
        source_objects=["{{ task_instance.xcom_pull(task_ids='extract_transform_and_load_to_gcs')['categories'] }}"],
        destination_project_dataset_table=f"{GCP_PROJECT_ID}.{BQ_DATASET_NAME}.staging_categories",
        source_format="PARQUET",
        write_disposition="WRITE_TRUNCATE",
        create_disposition="CREATE_IF_NEEDED",
        gcp_conn_id=GCP_CONN_ID,
    )
    load_companies_to_staging = GCSToBigQueryOperator(
        task_id="load_companies_to_staging",
        bucket=GCS_BUCKET_NAME,
        source_objects=["{{ task_instance.xcom_pull(task_ids='extract_transform_and_load_to_gcs')['companies'] }}"],
        destination_project_dataset_table=f"{GCP_PROJECT_ID}.{BQ_DATASET_NAME}.staging_companies",
        source_format="PARQUET",
        write_disposition="WRITE_TRUNCATE",
        create_disposition="CREATE_IF_NEEDED",
        gcp_conn_id=GCP_CONN_ID,
    )
    load_jobstats_to_staging = GCSToBigQueryOperator(
        task_id="load_jobstats_to_staging",
        bucket=GCS_BUCKET_NAME,
        source_objects=["{{ task_instance.xcom_pull(task_ids='extract_transform_and_load_to_gcs')['jobstats'] }}"],
        destination_project_dataset_table=f"{GCP_PROJECT_ID}.{BQ_DATASET_NAME}.staging_jobstats",
        source_format="PARQUET",
        write_disposition="WRITE_TRUNCATE",
        create_disposition="CREATE_IF_NEEDED",
        gcp_conn_id=GCP_CONN_ID,
    )
    load_locations_to_staging = GCSToBigQueryOperator(
        task_id="load_locations_to_staging",
        bucket=GCS_BUCKET_NAME,
        source_objects=["{{ task_instance.xcom_pull(task_ids='extract_transform_and_load_to_gcs')['locations'] }}"],
        destination_project_dataset_table=f"{GCP_PROJECT_ID}.{BQ_DATASET_NAME}.staging_locations",
        source_format="PARQUET",
        write_disposition="WRITE_TRUNCATE",
        create_disposition="CREATE_IF_NEEDED",
        gcp_conn_id=GCP_CONN_ID,
    )

    # --- Task Group 2: Merge Staging into Production ---
    merge_jobs_from_staging = BigQueryInsertJobOperator(
        task_id="merge_jobs_from_staging",
        configuration={"query": {"query": merge_jobs_sql, "useLegacySql": False}},
        gcp_conn_id=GCP_CONN_ID,
    )
    merge_categories_from_staging = BigQueryInsertJobOperator(
        task_id="merge_categories_from_staging",
        configuration={"query": {"query": merge_categories_sql, "useLegacySql": False}},
        gcp_conn_id=GCP_CONN_ID,
    )
    merge_companies_from_staging = BigQueryInsertJobOperator(
        task_id="merge_companies_from_staging",
        configuration={"query": {"query": merge_companies_sql, "useLegacySql": False}},
        gcp_conn_id=GCP_CONN_ID,
    )
    merge_jobstats_from_staging = BigQueryInsertJobOperator(
        task_id="merge_jobstats_from_staging",
        configuration={"query": {"query": merge_jobstats_sql, "useLegacySql": False}},
        gcp_conn_id=GCP_CONN_ID,
    )
    merge_locations_from_staging = BigQueryInsertJobOperator(
        task_id="merge_locations_from_staging",
        configuration={"query": {"query": merge_locations_sql, "useLegacySql": False}},
        gcp_conn_id=GCP_CONN_ID,
    )

    # --- Task Dependencies ---
    secrets_task = get_adzuna_secrets()
    gcs_load_task = extract_transform_and_load_to_gcs(secrets_task)

    gcs_load_task >> [
        load_jobs_to_staging,
        load_categories_to_staging,
        load_companies_to_staging,
        load_jobstats_to_staging,
        load_locations_to_staging
    ]

    load_jobs_to_staging >> merge_jobs_from_staging
    load_categories_to_staging >> merge_categories_from_staging
    load_companies_to_staging >> merge_companies_from_staging
    load_jobstats_to_staging >> merge_jobstats_from_staging
    load_locations_to_staging >> merge_locations_from_staging


adzuna_pipeline_merge()