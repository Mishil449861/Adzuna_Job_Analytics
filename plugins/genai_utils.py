import asyncio
import json
from datetime import datetime
from google.cloud import bigquery
from google.oauth2 import service_account
from openai import AsyncOpenAI

# Rate limit controller: Limits parallel requests to 10 at a time to avoid OpenAI errors
CONCURRENCY_LIMIT = 10 

async def process_single_job(client_openai, row, semaphore):
    """
    Process a single row asynchronously with a semaphore to control concurrency.
    """
    async with semaphore:
        job_text = f"Title: {row.title}\nDescription: {row.description}"
        ts = datetime.utcnow().isoformat()
        
        # --- 1. Enrichment (GPT-4o) ---
        enrich_prompt = f"""
        Extract structured information from this job posting.
        Return STRICT JSON with keys:
        skills (list of strings), years_min (int), years_max (int), 
        education (string), benefits (list of strings), tech_stack (list of strings).

        Job Posting:
        {job_text}
        """
        
        enrich_data = None
        try:
            enrich_resp = await client_openai.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": enrich_prompt}],
                response_format={"type": "json_object"}
            )
            raw_json = enrich_resp.choices[0].message.content
            parsed = json.loads(raw_json)

            enrich_data = {
                "job_id": row.job_id,
                "extracted_skills": parsed.get("skills", []),
                "years_experience_min": parsed.get("years_min"),
                "years_experience_max": parsed.get("years_max"),
                "education_level": parsed.get("education"),
                "benefits": parsed.get("benefits", []),
                "tech_stack": parsed.get("tech_stack", []),
                "processed_at": ts
            }
        except Exception as e:
            print(f"Enrichment error for job {row.job_id}: {e}")
            # Even if enrichment fails, we might want to return partial data or skip

        # --- 2. Embeddings (text-embedding-3-small) ---
        embed_data = None
        try:
            embed_resp = await client_openai.embeddings.create(
                model="text-embedding-3-small",
                input=job_text
            )
            vector = embed_resp.data[0].embedding
            
            embed_data = {
                "job_id": row.job_id,
                "embedding_vector": vector,
                "model_name": "text-embedding-3-small",
                "created_at": ts
            }
        except Exception as e:
            print(f"Embedding error for job {row.job_id}: {e}")

        return enrich_data, embed_data


def process_genai_data(GCP_PROJECT_ID, BQ_DATASET, gcp_service_account_json, openai_key):
    # --- Auth ---
    credentials = service_account.Credentials.from_service_account_file(gcp_service_account_json)
    client_bq = bigquery.Client(project=GCP_PROJECT_ID, credentials=credentials)
    
    # Init Async OpenAI Client
    client_openai = AsyncOpenAI(api_key=openai_key)

    JOBS_TABLE = f"{GCP_PROJECT_ID}.{BQ_DATASET}.jobs"
    ENRICH_TABLE = f"{GCP_PROJECT_ID}.{BQ_DATASET}.job_enrichment"
    EMBED_TABLE = f"{GCP_PROJECT_ID}.{BQ_DATASET}.job_embeddings"

    # --- INCREMENTAL LOGIC START ---
    # Only select jobs where 'job_id' is NOT in the enrichment table
    query = f"""
        SELECT j.job_id, j.title, j.description
        FROM `{JOBS_TABLE}` j
        LEFT JOIN `{ENRICH_TABLE}` e 
          ON j.job_id = e.job_id
        WHERE j.description IS NOT NULL
          AND e.job_id IS NULL
    """
    # --- INCREMENTAL LOGIC END ---

    print("Fetching new jobs from BigQuery...")
    jobs = list(client_bq.query(query).result())
    
    if not jobs:
        return "No new jobs found. Pipeline skipped."

    print(f"Found {len(jobs)} new jobs to process.")

    # --- Run Async Processing Loop ---
    async def run_batch():
        semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)
        tasks = [process_single_job(client_openai, row, semaphore) for row in jobs]
        return await asyncio.gather(*tasks)

    # Execute the async loop
    results = asyncio.run(run_batch())

    # --- Separate Successful Results ---
    enrichment_rows = [r[0] for r in results if r[0] is not None]
    embed_rows = [r[1] for r in results if r[1] is not None]

    # --- Insert into BigQuery ---
    if enrichment_rows:
        print(f"Inserting {len(enrichment_rows)} enrichment rows...")
        errors1 = client_bq.insert_rows_json(ENRICH_TABLE, enrichment_rows)
        if errors1: print(f"Enrichment Errors: {errors1}")

    if embed_rows:
        print(f"Inserting {len(embed_rows)} embedding rows...")
        errors2 = client_bq.insert_rows_json(EMBED_TABLE, embed_rows)
        if errors2: print(f"Embedding Errors: {errors2}")

    return f"Success. Processed {len(jobs)} jobs."