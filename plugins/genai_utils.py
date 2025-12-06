# plugins/genai_utils.py
import json
import logging
import google.generativeai as genai
from google.cloud import bigquery
import pandas as pd
import time

# Configure logging
logging.basicConfig(level=logging.INFO)

def get_gemini_client(api_key):
    """Configures the Gemini Client."""
    genai.configure(api_key=api_key)
    return genai

def fetch_unprocessed_jobs(project_id, dataset_id):
    """
    Fetches jobs from the main jobs table that do NOT exist 
    in the job_enrichment table yet.
    """
    client = bigquery.Client(project=project_id)
    # Check if job_enrichment exists; if not, fetch everything.
    # We use a LEFT JOIN to find jobs where e.job_id IS NULL.
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

def generate_embeddings_batch(text_list, api_key, model="models/text-embedding-004"):
    """Generates embeddings for a batch of text."""
    genai = get_gemini_client(api_key)
    embeddings = []
    
    for text in text_list:
        try:
            clean_text = text[:9000] if text else ""
            result = genai.embed_content(
                model=model,
                content=clean_text
            )
            embeddings.append(result['embedding'])
            time.sleep(0.5) 
        except Exception as e:
            logging.error(f"Embedding failed: {e}")
            embeddings.append(None)
            
    return embeddings

def extract_job_details(text, api_key):
    """
    Uses Gemini to extract structured JSON data from job descriptions.
    """
    genai = get_gemini_client(api_key)
    model = genai.GenerativeModel('gemini-1.5-flash', generation_config={"response_mime_type": "application/json"})
    
    prompt = f"""
    Analyze the following job description and extract the data into JSON format.
    Fields required: extracted_skills, years_experience_min, years_experience_max, education_level, benefits, tech_stack.
    
    Job Description:
    {text[:10000]}
    """
    
    try:
        response = model.generate_content(prompt)
        return json.loads(response.text)
    except Exception as e:
        logging.error(f"Extraction failed: {e}")
        return {}

# --- CRITICAL FIX: Add api_key here ---
def process_genai_data(project_id, dataset_id, api_key):
    """Main function to orchestrate the flow."""
    
    # 1. Get Data
    df = fetch_unprocessed_jobs(project_id, dataset_id)
    if df.empty:
        logging.info("No new jobs to process.")
        return "No Data"

    logging.info(f"Processing {len(df)} jobs...")

    embeddings_data = []
    enrichment_data = []
    
    # 2. Iterate and Process
    for index, row in df.iterrows():
        full_text = f"{row['title']} {row['description']}"
        
        # Pass api_key to internal functions
        emb = generate_embeddings_batch([full_text], api_key)[0]
        
        if emb:
            embeddings_data.append({
                "job_id": row['job_id'],
                "embedding_vector": emb,
                "model_name": "text-embedding-004",
                "created_at": pd.Timestamp.now()
            })

        details = extract_job_details(row['description'], api_key)
        if details:
            details['job_id'] = row['job_id']
            details['processed_at'] = pd.Timestamp.now()
            enrichment_data.append(details)

    # 3. Write to BigQuery (Robust Merge)
    client = bigquery.Client(project=project_id)

    def merge_data(df, table_name, primary_key="job_id"):
        if df.empty: return
        temp_table_id = f"{project_id}.{dataset_id}.temp_{table_name}_{int(time.time())}"
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE", autodetect=True)
        client.load_table_from_dataframe(df, temp_table_id, job_config=job_config).result()

        target_table_id = f"{project_id}.{dataset_id}.{table_name}"
        cols = df.columns.tolist()
        col_string = ", ".join(cols)
        
        query = f"""
        MERGE `{target_table_id}` T
        USING `{temp_table_id}` S
        ON T.{primary_key} = S.{primary_key}
        WHEN NOT MATCHED THEN
          INSERT ({col_string}) VALUES ({col_string})
        """
        client.query(query).result()
        client.delete_table(temp_table_id, not_found_ok=True)

    if embeddings_data:
        merge_data(pd.DataFrame(embeddings_data), "job_embeddings")

    if enrichment_data:
        merge_data(pd.DataFrame(enrichment_data), "job_enrichment")

    return "Success"