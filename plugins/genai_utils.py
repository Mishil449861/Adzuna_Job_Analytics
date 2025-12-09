from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime
import openai

def process_genai_data(
    GCP_PROJECT_ID: str,
    BQ_DATASET: str,
    gcp_service_account_json: str,
    openai_key: str,
) -> str:
    """
    Reads jobs table → generates enrichment + embeddings → appends to:
        - job_enrichment
        - job_embeddings
    """

    # --- Auth ---
    credentials = service_account.Credentials.from_service_account_file(
        gcp_service_account_json
    )
    client_bq = bigquery.Client(project=GCP_PROJECT_ID, credentials=credentials)

    openai.api_key = openai_key

    # --- Tables ---
    JOBS_TABLE = f"{GCP_PROJECT_ID}.{BQ_DATASET}.jobs"
    ENRICH_TABLE = f"{GCP_PROJECT_ID}.{BQ_DATASET}.job_enrichment"
    EMBED_TABLE = f"{GCP_PROJECT_ID}.{BQ_DATASET}.job_embeddings"

    # --- Load jobs ---
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

        # --- Enrichment Prompt ---
        enrich_prompt = f"""
        Extract skills, years of experience required (min and max), education level,
        benefits, and tech stack from this job posting. Respond in JSON with keys:
        [skills, years_min, years_max, education, benefits, tech_stack]

        Job Posting:
        {job_text}
        """

        enrich_resp = openai.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": enrich_prompt}]
        )

        data = enrich_resp.choices[0].message.content
        parsed = eval(data)  # assume safe JSON-like output

        enrichment_rows.append({
            "job_id": row.job_id,
            "extracted_skills": parsed.get("skills", []),
            "years_experience_min": parsed.get("years_min"),
            "years_experience_max": parsed.get("years_max"),
            "education_level": parsed.get("education"),
            "benefits": parsed.get("benefits", []),
            "tech_stack": parsed.get("tech_stack", []),
            "processed_at": datetime.utcnow().isoformat()
        })

        # --- Embedding ---
        embed_resp = openai.embeddings.create(
            model="text-embedding-3-large",
            input=job_text
        )

        vector = embed_resp.data[0].embedding

        embed_rows.append({
            "job_id": row.job_id,
            "embedding_vector": vector,
            "model_name": "text-embedding-3-large",
            "created_at": datetime.utcnow().isoformat()
        })

    # --- Load to BigQuery ---
    enrich_errors = client_bq.insert_rows_json(ENRICH_TABLE, enrichment_rows)
    embed_errors = client_bq.insert_rows_json(EMBED_TABLE, embed_rows)

    if enrich_errors or embed_errors:
        return f"Errors: {enrich_errors} {embed_errors}"

    return f"Success — Inserted {len(enrichment_rows)} rows into both tables."