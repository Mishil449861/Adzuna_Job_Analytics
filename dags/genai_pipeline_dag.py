import os
import logging
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from google.cloud import secretmanager
from google.oauth2 import service_account
import sys
from genai_utils import process_genai_data

# Add plugins folder to path to import the script we made in Step 2
dag_dir = os.path.dirname(os.path.abspath(__file__))
plugins_dir = os.path.join(os.path.dirname(dag_dir), 'plugins')
if plugins_dir not in sys.path:
    sys.path.insert(0, plugins_dir)

from genai_utils import process_genai_data

# --- Config ---
GCP_PROJECT_ID = "ba882-team4-474802"
BQ_DATASET_NAME = "ba882_jobs"
SECRET_GEMINI_KEY = "OPEN_AI_API" # Make sure this exists in Secret Manager

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id="adzuna_genai_enrichment_v1",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule="30 0 * * *", # Runs at 12:30 AM (30 mins after your main DAG)
    catchup=False,
    tags=["genai", "embeddings", "llm", "enrichment"],
)
def genai_enrichment_dag():

    @task
    def get_gemini_secret() -> str:
        # Authentication to get the secret
        key_path = "/usr/local/airflow/include/ba882-team4-474802-bee53a65f2ac.json"
        credentials = service_account.Credentials.from_service_account_file(key_path)
        client = secretmanager.SecretManagerServiceClient(credentials=credentials)
        
        secret_name = f"projects/{GCP_PROJECT_ID}/secrets/{SECRET_GEMINI_KEY}/versions/latest"
        response = client.access_secret_version(request={"name": secret_name})
        return response.payload.data.decode("UTF-8")

    @task
    def run_genai_processing(api_key: str):
        # Calls the utility script to process data and load to BQ
        status = process_genai_data(GCP_PROJECT_ID, BQ_DATASET_NAME, api_key)
        logging.info(f"GenAI Processing Status: {status}")

    # Flow
    api_key = get_gemini_secret()
    run_genai_processing(api_key)

genai_enrichment_dag()