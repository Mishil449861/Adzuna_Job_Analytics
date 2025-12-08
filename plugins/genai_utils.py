# plugins/genai_utils.py
import logging
from datetime import datetime
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from openai import OpenAI
import json
import time

SERVICE_ACCOUNT_PATH = "/usr/local/airflow/include/ba882-team4-474802-bee53a65f2ac.json"

EMBEDDING_MODEL = "text-embedding-3-small"
CHAT_MODEL = "gpt-4o-mini"  # or another chat model you prefer

def get_bq_credentials():
    return service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_PATH,
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )

def fetch_unprocessed_jobs(project_id, dataset_id, limit=50):
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
        LIMIT {limit}
    """
    return client.query(query).to_dataframe()

def extract_skills_with_openai(openai_client: OpenAI, text: str):
    # Ask the chat model to return a JSON list of skills only.
    system = "You are an assistant that extracts concise skill keywords from a job description. Output only valid JSON: an array of lowercase skill strings. Do not include extra commentary."
    user = f"Job description:\n\n{text}\n\nReturn a JSON array of the top 8 skills (single words or short phrases)."

    resp = openai_client.chat.completions.create(
        model=CHAT_MODEL,
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": user}
        ],
        max_tokens=200,
        temperature=0.0
    )
    raw = resp.choices[0].message.content.strip()

    # Defensive parse: try to parse JSON, fallback to simple heuristics
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, list):
            skills = [s.strip() for s in parsed if isinstance(s, str) and s.strip()]
            return skills[:8]
    except Exception:
        logging.warning("OpenAI returned non-JSON skills: %s", raw)

    # fallback: simple token selection
    tokens = [w.strip(".,()") for w in text.split() if len(w) > 3]
    top = list(dict.fromkeys(tokens))[:8]
    return top

def process_genai_data(project_id: str, dataset_id: str, api_key: str):
    logging.info("Starting genai enrichment process")
    df = fetch_unprocessed_jobs(project_id, dataset_id)
    if df.empty:
        logging.info("No new jobs to process.")
        return "No Data"

    openai_client = OpenAI(api_key=api_key)
    bq_client = bigquery.Client(project=project_id, credentials=get_bq_credentials())

    embeddings_table = f"{project_id}.{dataset_id}.job_embeddings"
    enrichment_table = f"{project_id}.{dataset_id}.job_enrichment"

    rows_embeddings = []
    rows_enrichment = []

    for _, row in df.iterrows():
        job_id = str(row["job_id"])
        title = row.get("title", "") or ""
        desc = row.get("description", "") or ""
        text_for_embedding = (title + "\n\n" + desc)[:32000]  # safe limit

        # Create embedding
        try:
            emb_resp = openai_client.embeddings.create(
                model=EMBEDDING_MODEL,
                input=text_for_embedding
            )
            embedding = emb_resp.data[0].embedding
        except Exception as e:
            logging.exception("Embedding generation failed for job_id %s: %s", job_id, e)
            continue

        # Extract skills (chat)
        try:
            skills = extract_skills_with_openai(openai_client, desc)
        except Exception as e:
            logging.exception("Skill extraction failed for job_id %s: %s", job_id, e)
            skills = []

        now_ts = datetime.utcnow().isoformat()

        rows_embeddings.append({
            "job_id": job_id,
            "embedding_vector": embedding,
            "model_name": EMBEDDING_MODEL,
            "created_at": now_ts
        })

        rows_enrichment.append({
            "job_id": job_id,
            "extracted_skills": skills,
            "processed_at": now_ts
        })

        # Rate limit safety
        time.sleep(0.2)

    # Insert embeddings in bulk (use insert_rows_json)
    if rows_embeddings:
        table = bq_client.get_table(embeddings_table)
        errors = bq_client.insert_rows_json(table, rows_embeddings)
        if errors:
            logging.error("Errors inserting embeddings: %s", errors)
        else:
            logging.info("Inserted %d embeddings.", len(rows_embeddings))

    # Insert enrichment rows (job_enrichment)
    if rows_enrichment:
        table_e = bq_client.get_table(enrichment_table)
        errors_e = bq_client.insert_rows_json(table_e, rows_enrichment)
        if errors_e:
            logging.error("Errors inserting job_enrichment rows: %s", errors_e)
        else:
            logging.info("Inserted %d enrichment rows.", len(rows_enrichment))

    return {"embeddings_inserted": len(rows_embeddings), "enrichment_inserted": len(rows_enrichment)}
