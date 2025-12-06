import json
import logging
import google.generativeai as genai
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import time

SERVICE_ACCOUNT_PATH = "/usr/local/airflow/include/ba882-team4-474802-bee53a65f2ac.json"

def get_bq_credentials():
    return service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_PATH,
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )

def fetch_unprocessed_jobs(project_id, dataset_id):
    client = bigquery.Client(project=project_id, credentials=get_bq_credentials())
    query = f"""
        SELECT 
            j.job_id,
            j.title,
            j.description
        FROM `{project_id}.{dataset_id}.jobs` j
        LEFT JOIN `{project_id}.{dataset_id}.job_enrichment` e
        ON j.job_id = e.job_id
        WHERE e.job_id IS NULL
        LIMIT 50
    """
    return client.query(query).to_dataframe()

def process_genai_data(project_id, dataset_id, api_key):
    df = fetch_unprocessed_jobs(project_id, dataset_id)
    if df.empty:
        logging.info("No new jobs to process.")
        return "No Data"

    client = bigquery.Client(project=project_id, credentials=get_bq_credentials())
    ...