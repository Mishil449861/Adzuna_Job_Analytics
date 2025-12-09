import re
import unicodedata
import numpy as np
from google.cloud import bigquery
from openai import OpenAI


EMBEDDING_MODEL = "text-embedding-3-large"
SKILL_EXTRACTION_MODEL = "gpt-4o-mini"


# --------------------------------------------------
# Text Cleaning
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
# Normalize Vector
# --------------------------------------------------
def normalize_vector(v):
    v = np.array(v)
    n = np.linalg.norm(v)
    return (v / n).tolist() if n > 0 else v.tolist()


# --------------------------------------------------
# Skill Extraction using LLM
# --------------------------------------------------
def extract_skills_llm(openai_client, title: str, desc: str):
    prompt = f"""
    Extract the key job-relevant skills from this job posting.
    Return them as a comma-separated list only.

    Job Title:
    {title}

    Description:
    {desc}
    """

    resp = openai_client.chat.completions.create(
        model=SKILL_EXTRACTION_MODEL,
        messages=[{"role": "user", "content": prompt}],
        temperature=0.0
    )

    text = resp.choices[0].message["content"]

    skills = [s.strip() for s in text.split(",") if len(s.strip()) > 1]
    return skills[:20]   # cap to avoid bloated embeddings


# --------------------------------------------------
# MAIN PROCESS
# --------------------------------------------------
def process_genai_data(GCP_PROJECT_ID: str, BQ_DATASET: str, openai_key: str) -> str:
    """
    1. Read job postings from BigQuery
    2. Extract skills
    3. Clean text
    4. Create enhanced embedding
    5. Normalize embedding
    6. Write back to BigQuery table job_enriched
    """
    client_bq = bigquery.Client(project=GCP_PROJECT_ID)
    openai_client = OpenAI(api_key=openai_key)

    # --------------------------------------------------
    # Load source data
    # --------------------------------------------------
    query = f"""
        SELECT job_id, job_title, job_description
        FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.job_raw`
        WHERE job_title IS NOT NULL
    """
    rows = client_bq.query(query).result()

    output_rows = []

    for row in rows:
        title = row["job_title"]
        desc = row["job_description"]

        # ---- Clean text ----
        title_clean = clean_text(title)
        desc_clean = clean_text(desc)

        # ---- LLM Skill Extraction ----
        skills = extract_skills_llm(openai_client, title_clean, desc_clean)
        skills_text = ", ".join(skills)

        # ---- Construct Embedding Input ----
        enriched_text = f"""
        Title: {title_clean}
        Skills: {skills_text}
        Description: {desc_clean}
        """

        enriched_text = enriched_text[:32000]

        # ---- Embedding ----
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
    # Write results to BigQuery
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