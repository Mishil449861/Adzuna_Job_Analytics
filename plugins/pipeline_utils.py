# plugins/genai_utils.py
import re
import unicodedata
import numpy as np
from google.cloud import bigquery
from openai import OpenAI

EMBEDDING_MODEL = "text-embedding-3-large"   # upgraded
SKILL_MODEL = "gpt-4o-mini"                   # keep same

# ------------------------------------------------------
# Text Cleaning
# ------------------------------------------------------
def clean_text_basic(text: str) -> str:
    if not isinstance(text, str):
        return ""
    text = unicodedata.normalize("NFKC", text)
    text = re.sub(r"\s+", " ", text)
    text = re.sub(r"(.)\1{4,}", r"\1", text)
    text = re.sub(r"(?i)equal opportunity.*", "", text)
    text = re.sub(r"(?i)copyright.*|all rights reserved.*", "", text)
    return text.strip()

# ------------------------------------------------------
# Vector Normalization
# ------------------------------------------------------
def normalize_vector(v):
    v = np.array(v)
    n = np.linalg.norm(v)
    return (v / n).tolist() if n > 0 else v.tolist()

# ------------------------------------------------------
# Extract Skills (unchanged)
# ------------------------------------------------------
def extract_skills(client, title, description):
    system_message = (
        "Extract and return only a list of skills from the following job title and description. "
        "No extra text, return as a Python list of strings."
    )

    content = f"Title: {title}\n\nDescription: {description}"

    resp = client.chat.completions.create(
        model=SKILL_MODEL,
        messages=[
            {"role": "system", "content": system_message},
            {"role": "user", "content": content},
        ],
        max_tokens=200,
    )

    text = resp.choices[0].message.content.strip()

    try:
        skills = eval(text)
        if isinstance(skills, list):
            return skills
    except Exception:
        pass

    return []

# ------------------------------------------------------
# Main Job Processing Pipeline
# ------------------------------------------------------
def process_genai_data(project_id: str, dataset_name: str, api_key: str):
    client_bq = bigquery.Client(project=project_id)
    openai_client = OpenAI(api_key=api_key)

    source_table = f"{project_id}.{dataset_name}.raw_jobs"
    target_table = f"{project_id}.{dataset_name}.genai_jobs"

    # Get all jobs with missing embeddings
    query = f"""
        SELECT job_id, title, description
        FROM `{source_table}`
        WHERE job_id NOT IN (
            SELECT job_id FROM `{target_table}`
        )
    """

    rows = client_bq.query(query).result()

    processed_rows = []

    for row in rows:
        job_id = row.job_id
        title = row.title or ""
        desc = row.description or ""

        # Clean text
        cleaned_text = clean_text_basic(title + " " + desc)

        # Extract skills
        skills = extract_skills(openai_client, title, desc)
        skill_text = ", ".join(skills)

        # Build enhanced embedding text
        final_text = f"{title}\n\nSkills: {skill_text}\n\n{cleaned_text}"[:32000]

        # Create embedding
        emb_resp = openai_client.embeddings.create(
            model=EMBEDDING_MODEL,
            input=final_text
        )
        vector = normalize_vector(emb_resp.data[0].embedding)

        processed_rows.append({
            "job_id": job_id,
            "title": title,
            "description": desc,
            "skills": skills,
            "embedding_vector": vector,
        })

    # Write to BigQuery
    if processed_rows:
        errors = client_bq.insert_rows_json(target_table, processed_rows)
        if errors:
            raise RuntimeError(f"BigQuery insert errors: {errors}")

    return f"Processed {len(processed_rows)} jobs"