# plugins/genai_utils.py
import os
import json
import logging
import time
import pandas as pd
from google.cloud import bigquery, secretmanager
from openai import OpenAI

# Configure logging
logging.basicConfig(level=logging.INFO)

# -----------------------------
# SECRET MANAGER
# -----------------------------
def load_openai_key(project_id, secret_name="OPEN_AI_API", version="1"):
    """Loads API key from Google Secret Manager."""
    client = secretmanager.SecretManagerServiceClient()
    secret_path = f"projects/{project_id}/secrets/{secret_name}/versions/{version}"
    response = client.access_secret_version(name=secret_path)
    return response.payload.data.decode("utf-8")


def get_openai_client(project_id):
    """Builds OpenAI client using Secret Manager."""
    api_key = load_openai_key(project_id)
    return OpenAI(api_key=api_key)

# -----------------------------
# FETCH DATA FROM BIGQUERY
# -----------------------------
def fetch_unprocessed_jobs(project_id, dataset_id):
    client = bigquery.Client(project=project_id)
    query = f"""
        SELECT 
            j.job_id,
            j.title,
            j.description,
            j.company_name,
            j.city,
            j.state
        FROM `{project_id}.{dataset_id}.jobs` j
        LEFT JOIN `{project_id}.{dataset_id}.job_enrichment` e
        ON j.job_id = e.job_id
        WHERE e.job_id IS NULL
        LIMIT 50
    """
    return client.query(query).to_dataframe()

# -----------------------------
# EMBEDDINGS
# -----------------------------
def generate_embeddings_batch(text_list, project_id, model="text-embedding-3-small"):
    client = get_openai_client(project_id)
    embeddings = []

    for text in text_list:
        try:
            clean_text = text[:9000] if text else ""
            response = client.embeddings.create(
                model=model,
                input=clean_text
            )
            embeddings.append(response.data[0].embedding)
            time.sleep(0.3)
        except Exception as e:
            logging.error(f"Embedding failed: {e}")
            embeddings.append(None)

    return embeddings

# -----------------------------
# EXTRACT STRUCTURED JOB INFO
# -----------------------------
def extract_job_details(text, project_id):
    client = get_openai_client(project_id)

    prompt = f"""
    Extract structured information from the job description below
    and return ONLY valid JSON.

    Required fields:
    - extracted_skills (list of strings)
    - years_experience_min (integer, or 0)
    - years_experience_max (integer, or 0)
    - education_level (string: Bachelors, Masters, PhD, None)
    - benefits (list of strings)
    - tech_stack (list of tools/technologies)

    Job Description:
    {text[:9000]}
    """

    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "user", "content": prompt}
            ],
            temperature=0
        )

        return json.loads(response.choices[0].message["content"])
    except Exception as e:
        logging.error(f"Extraction failed: {e}")
        return {
            "extracted_skills": [],
            "years_experience_min": 0,
            "years_experience_max": 0,
            "education_level": None,
            "benefits": [],
            "tech_stack": []
        }

# -----------------------------
# MAIN PROCESSING LOGIC
# -----------------------------
def process_genai_data(project_id, dataset_id):
    df = fetch_unprocessed_jobs(project_id, dataset_id)
    if df.empty:
        logging.info("No new jobs to process.")
        return "No Data"

    logging.info(f"Processing {len(df)} jobs...")

    embeddings_data = []
    enrichment_data = []

    for _, row in df.iterrows():
        full_text = f"{row['title']} {row['description']}"

        # A. Embeddings
        emb = generate_embeddings_batch([full_text], project_id)[0]
        if emb:
            embeddings_data.append({
                "job_id": row["job_id"],
                "embedding_vector": emb,
                "model_name": "text-embedding-3-small",
                "created_at": pd.Timestamp.now()
            })

        # B. Job Enrichment
        details = extract_job_details(row["description"], project_id)
        details["job_id"] = row["job_id"]
        details["processed_at"] = pd.Timestamp.now()
        enrichment_data.append(details)

    # WRITE TO BIGQUERY
    client = bigquery.Client(project=project_id)

    if embeddings_data:
        emb_df = pd.DataFrame(embeddings_data)
        client.load_table_from_dataframe(
            emb_df, f"{project_id}.{dataset_id}.job_embeddings"
        ).result()
        logging.info(f"Uploaded {len(emb_df)} embeddings.")

    if enrichment_data:
        enr_df = pd.DataFrame(enrichment_data)
        client.load_table_from_dataframe(
            enr_df, f"{project_id}.{dataset_id}.job_enrichment"
        ).result()
        logging.info(f"Uploaded {len(enr_df)} enrichment records.")

    return "Success"