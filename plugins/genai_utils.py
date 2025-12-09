from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime
from openai import OpenAI
import json


def process_genai_data(
    GCP_PROJECT_ID: str,
    BQ_DATASET: str,
    gcp_service_account_json: str,
    openai_key: str,
) -> str:
    """
    Reads BQ jobs table → generates enrichment + embeddings → inserts into:
      - job_enrichment
      - job_embeddings
    """

    # --- Auth ---
    credentials = service_account.Credentials.from_service_account_file(
        gcp_service_account_json
    )
    client_bq = bigquery.Client(project=GCP_PROJECT_ID, credentials=credentials)
    client_openai = OpenAI(api_key=OPEN_AI_API)

    # --- Tables ---
    JOBS_TABLE = f"{GCP_PROJECT_ID}.{BQ_DATASET}.jobs"
    ENRICH_TABLE = f"{GCP_PROJECT_ID}.{BQ_DATASET}.job_enrichment"
    EMBED_TABLE = f"{GCP_PROJECT_ID}.{BQ_DATASET}.job_embeddings"

    # --- Load job postings ---
    query = f"""
        SELECT job_id, title, description
        FROM `{JOBS_TABLE}`
        WHERE description IS NOT NULL
    """
    jobs = client_bq.query(query).result()

    enrichment_rows = []
    embed_rows = []

    for row in jobs:
        job_text = f"Title: {row.title}\nDescription: {row.description}"
        ts = datetime.utcnow().isoformat()

        # -----------------------------------------------------
        # 1. LLM Enrichment (Safe JSON Parsing)
        # -----------------------------------------------------
        enrich_prompt = f"""
        Extract structured information from this job posting.
        Return STRICT JSON with keys:
        skills, years_min, years_max, education, benefits, tech_stack.

        Job Posting:
        {job_text}
        """

        try:
            enrich_resp = client_openai.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": enrich_prompt}],
                response_format={"type": "json_object"}
            )

            raw_json = enrich_resp.choices[0].message.content
            parsed = json.loads(raw_json)

        except Exception as e:
            parsed = {
                "skills": [],
                "years_min": None,
                "years_max": None,
                "education": None,
                "benefits": [],
                "tech_stack": []
            }

        enrichment_rows.append({
            "job_id": row.job_id,
            "extracted_skills": parsed.get("skills", []),
            "years_experience_min": parsed.get("years_min"),
            "years_experience_max": parsed.get("years_max"),
            "education_level": parsed.get("education"),
            "benefits": parsed.get("benefits", []),
            "tech_stack": parsed.get("tech_stack", []),
            "processed_at": ts
        })

        # -----------------------------------------------------
        # 2. Embedding Generation
        # -----------------------------------------------------
        try:
            embed_resp = client_openai.embeddings.create(
                model="text-embedding-3-large",
                input=job_text
            )

            vector = embed_resp.data[0].embedding

        except Exception:
            vector = []

        embed_rows.append({
            "job_id": row.job_id,
            "embedding_vector": vector,
            "model_name": "text-embedding-3-large",
            "created_at": ts
        })

    # -----------------------------------------------------
    # 3. Insert into BigQuery
    # -----------------------------------------------------
    enrich_errors = client_bq.insert_rows_json(ENRICH_TABLE, enrichment_rows)
    embed_errors = client_bq.insert_rows_json(EMBED_TABLE, embed_rows)

    if enrich_errors or embed_errors:
        return f"Errors: {enrich_errors} {embed_errors}"

    return f"Success — Inserted {len(enrichment_rows)} rows into enrichment & embeddings tables."