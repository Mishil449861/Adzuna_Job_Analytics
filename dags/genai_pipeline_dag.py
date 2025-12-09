from datetime import datetime, timedelta
import logging
from airflow import DAG
from airflow.decorators import task
from google.cloud import secretmanager
from google.oauth2 import service_account
# Ensure this import matches your folder structure
from plugins.genai_utils import process_genai_data

# Configuration
GCP_PROJECT_ID = "ba882-team4-474802"
BQ_DATASET_NAME = "ba882_jobs"
SECRET_OPENAI_KEY = "OPEN_AI_API"
SERVICE_ACCOUNT_PATH = "/usr/local/airflow/include/ba882-team4-474802-bee53a65f2ac.json"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 3,  # Lower retries for debugging
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id="genai_enrichment_openai_v2", # Bumped version for clarity
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule="30 0 * * *",
    catchup=False,
    tags=["genai", "embeddings", "openai"],
) as dag:

    @task()
    def get_openai_secret() -> str:
        # Load credentials for Secret Manager
        credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_PATH)
        client = secretmanager.SecretManagerServiceClient(credentials=credentials)
        
        secret_name = f"projects/{GCP_PROJECT_ID}/secrets/{SECRET_OPENAI_KEY}/versions/latest"
        response = client.access_secret_version(request={"name": secret_name})
        return response.payload.data.decode("UTF-8")

    @task()
    def run_genai_processing(api_key: str):
        # Call the utils function
        status = process_genai_data(
            GCP_PROJECT_ID=GCP_PROJECT_ID,
            BQ_DATASET=BQ_DATASET_NAME,
            gcp_service_account_json=SERVICE_ACCOUNT_PATH,
            openai_key=api_key
        )
        logging.info(f"GenAI Pipeline Finished: {status}")

    # DAG Flow
    key = get_openai_secret()
    run_genai_processing(key)