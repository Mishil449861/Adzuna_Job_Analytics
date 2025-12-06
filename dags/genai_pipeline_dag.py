import os
import sys
import logging
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from google.cloud import secretmanager
from google.oauth2 import service_account

# --- Import Fix: Setup Path BEFORE importing from plugins ---
# 1. Get the folder where this DAG lives
current_dir = os.path.dirname(os.path.abspath(__file__))
# 2. Go up one level (to airflow root) and into 'plugins'
plugins_dir = os.path.join(os.path.dirname(current_dir), 'plugins')

# 3. Add to Python path if not already there
if os.path.exists(plugins_dir) and plugins_dir not in sys.path:
    sys.path.insert(0, plugins_dir)
else:
    # Fallback for some setups
    hardcoded_path = "/usr/local/airflow/plugins"
    if hardcoded_path not in sys.path:
        sys.path.insert(0, hardcoded_path)

# 4. NOW import the module (this prevents ModuleNotFoundError)
try:
    from genai_utils import process_genai_data
except ImportError:
    logging.error("Could not import genai_utils. Make sure the file exists in plugins/")
    raise

# --- Config ---
GCP_PROJECT_ID = "ba882-team4-474802"
BQ_DATASET_NAME = "ba882_jobs"
# Note: Ensure the secret in Secret Manager contains your GEMINI API Key
SECRET_GEMINI_KEY = "OPEN_AI_API" 
GCP_CONN_ID = "gcp_default"

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
    schedule="30 0 * * *", 
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
        # CRITICAL: Ensure process_genai_data in 'plugins/genai_utils.py' 
        # is defined as: def process_genai_data(project_id, dataset_id, api_key):
        status = process_genai_data(GCP_PROJECT_ID, BQ_DATASET_NAME, api_key)
        logging.info(f"GenAI Processing Status: {status}")

    # Flow
    api_key = get_gemini_secret()
    run_genai_processing(api_key)

genai_enrichment_dag()