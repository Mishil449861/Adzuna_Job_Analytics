import re
import unicodedata
import numpy as np
from google.cloud import bigquery
from google.oauth2 import service_account
from openai import OpenAI

EMBEDDING_MODEL = "text-embedding-3-large"
SKILL_EXTRACTION_MODEL = "gpt-4o-mini"

# --------------------------------------------------
# Clean Text
# --------------------------------------------------
def clean_text(text: str) -> str:
    if not isinstance(text, str):
        return ""
    text = unicodedata.normalize("NFKC", text)
    text = re.sub(r"\s+", " ", text)
    text = re.sub(r"(.)\1{4,}", r"\1", text)
    text = re.sub(r"(?i)apply now.*", "", text)
    text = re.sub(r"(?i)equal opportunity employer.*", "", text)
    text = re.sub(r"(?i)legal disclaimer.*", "", text)
    return text.strip()


# --------------------------------------------------
# Normalize Embedding Vector
# --------------------------------------------------
def normalize_vector(v):
    v = np.array(v)
    n = np.linalg.norm(v)
    return (v / n).tolist() if n > 0 else v.tolist()


# --------------------------------------------------
# Skill Extraction via LLM
# --------------------------------------------------
def extract_skills_llm(openai_client, title: str, desc: str):
    prompt = f"""
    Extract the key job-relevant skills from this posting.
    Return skills ONLY as a comma-separated list.

    Title:
    {title}

    Description:
    {desc}
    """

    resp = openai_client.chat.completions.create(
        model=SKILL_EXTRACTION_MODEL,
        messages=[{"role": "user", "content": prompt}],
        temperature=0.0
    )

    raw = resp.choices[0].message["content"]
    skills = [s.strip() for s in raw.split(",") if 1 < len(s.strip()) < 50]

    return skills[:20]


# --------------------------------------------------
# Main Processing Flow
# --------------------------------------------------
def process_genai_data(GCP_PROJECT_ID: str, BQ_DATASET: str, openai_key: str) -> str:
    """
    1. Read job postings from BigQuery
    2. Clean text
    3. Extract skills via OpenAI
    4. Generate embeddings
    5. Normalize embeddings
    6. Write enriched rows back to BigQuery
    """

    # -------- Use YOUR service account credentials --------
    key_path = "/usr/local/airflow/include/ba882-team4-474802-bee53a65f2ac.json"
    credentials = service_account.Credentials.from_service_account_file(key_path)

    client_bq = bigquery.Client(
        project=GCP_PROJECT_ID,
        credentials=credentials
    )

    openai_client = OpenAI(api_key=openai_key)

    # -------- Load Source Data --------
    query = f"""
        SELECT job_id, job_title, job_description
        FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.job_raw`
        WHERE job_title IS NOT NULL
        LIMIT 5000
    """
    rows = client_bq.query(query).result()

    output_rows = []

    for row in rows:
        title = row["job_title"]
        desc = row["job_description"]

        title_clean = clean_text(title)
        desc_clean = clean_text(desc)

        # ---- Extract skills from LLM ----
        skills = extract_skills_llm(openai_client, title_clean, desc_clean)
        skills_text = ", ".join(skills)

        # ---- Construct text for embedding ----
        enriched_text = f"Title: {title_clean}\nSkills: {skills_text}\nDescription: {desc_clean}"
        enriched_text = enriched_text[:30000]  # token safety

        # ---- Generate embedding ----
        emb = openai_client.embeddings.create(
            model=EMBEDDING_MODEL,
            input=enriched_text
        )

        normalized_vec = normalize_vector(emb.data[0].embedding)

        output_rows.append({
            "job_id": row["job_id"],
            "job_title": title_clean,
            "job_description": desc_clean,
            "skills": skills_text,
            "embedding_vector": normalized_vec
        })

    # --------------------------------------------------
    # Write enriched rows back to BigQuery
    # --------------------------------------------------
    table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET}.job_enriched"

    job = client_bq.load_table_from_json(
        output_rows,
        table_id,
        job_config=bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE"
        )
    )
    job.result()

    return f"Processed {len(output_rows)} job rows."