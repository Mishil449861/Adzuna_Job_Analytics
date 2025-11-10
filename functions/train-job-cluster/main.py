import functions_framework
import pandas as pd
import joblib
import io
from datetime import datetime
from google.cloud import bigquery, storage
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.cluster import KMeans

# --- Configuration ---
PROJECT_ID = "ba882-team4-474802"
DATASET_ID = "ba882_jobs"
JOBS_TABLE = f"{PROJECT_ID}.{DATASET_ID}.jobs"
SKILLS_TABLE = f"{PROJECT_ID}.{DATASET_ID}.job_skills"
CLUSTER_TABLE = f"{PROJECT_ID}.{DATASET_ID}.job_clusters"
GCS_BUCKET = "adzuna-bucket"
MODEL_VERSION = f"kmeans_k10_{datetime.now().strftime('%Y%m%d')}"
KEY_FILE = "/usr/local/airflow/include/ba882-team4-474802-bee53a65f2ac.json" # Assumes function uses same service account

# --- Queries ---
JOBS_QUERY = f"SELECT job_id, title FROM `{JOBS_TABLE}`"
SKILLS_QUERY = f"SELECT source_job_id, skill_name FROM `{SKILLS_TABLE}`"

def pipe_tokenizer(s):
    """Helper function to tokenize the skill string"""
    return s.split('|')

@functions_framework.http
def train_cluster_model(request):
    """
    HTTP-triggered Cloud Function to:
    1. Load data from BigQuery
    2. Train K-Means clustering model
    3. Save model artifacts to GCS
    4. Save cluster assignments to BigQuery
    """
    print("Starting job clustering model training...")
    
    # Authenticate and create clients
    # Note: In a real Cloud Function, you'd typically use the function's
    # runtime service account, not a key file. But for simplicity, we
    # can re-use the one from Airflow *if* it's built into the function's container.
    # A better way is to grant the function's service account
    # "BigQuery Data Editor" and "Storage Object Admin" roles.
    
    # For this plan, we'll assume the function's runtime service account
    # has the required IAM roles (BigQuery Data Editor, Storage Object Admin).
    
    bq_client = bigquery.Client(project=PROJECT_ID)
    storage_client = storage.Client(project=PROJECT_ID)
    
    # 1. Load data from BigQuery
    print(f"Loading data from {JOBS_TABLE} and {SKILLS_TABLE}...")
    try:
        jobs_df = bq_client.query(JOBS_QUERY).to_dataframe()
        skills_df = bq_client.query(SKILLS_QUERY).to_dataframe()
    except Exception as e:
        print(f"Error loading data from BigQuery: {e}")
        return (f"BigQuery load error: {e}", 500)

    # 2. Process data (from your notebook)
    print("Processing data...")
    jobs_merged = jobs_df.merge(
        skills_df,
        left_on="job_id",
        right_on="source_job_id",
        how="left"
    )
    jobs_with_skills = jobs_merged[jobs_merged['skill_name'].notna()].copy()
    job_skill_groups = (
        jobs_with_skills
        .groupby('job_id')['skill_name']
        .apply(lambda x: list(set(x.dropna())))
        .reset_index()
    )
    job_skill_groups['skill_text'] = job_skill_groups['skill_name'].apply(lambda x: '|'.join(x))

    if job_skill_groups.empty:
        return ("No jobs with skills found to cluster.", 200)

    # 3. Train K-Means model (from your notebook)
    print("Training K-Means model...")
    vectorizer = CountVectorizer(tokenizer=pipe_tokenizer, binary=True)
    X = vectorizer.fit_transform(job_skill_groups['skill_text'])
    
    kmeans = KMeans(n_clusters=10, random_state=42, n_init=10)
    job_skill_groups['cluster_id'] = kmeans.fit_predict(X)

    # 4. Save model artifacts to GCS
    print(f"Saving artifacts to GCS bucket {GCS_BUCKET}...")
    bucket = storage_client.bucket(GCS_BUCKET)
    
    # Save Vectorizer
    blob_vect = bucket.blob(f"ml_artifacts/{MODEL_VERSION}/count_vectorizer.pkl")
    with blob_vect.open("wb") as f:
        joblib.dump(vectorizer, f)
        
    # Save Model
    blob_model = bucket.blob(f"ml_artifacts/{MODEL_VERSION}/kmeans_model.pkl")
    with blob_model.open("wb") as f:
        joblib.dump(kmeans, f)

    # 5. Save cluster assignments to BigQuery
    print(f"Saving cluster assignments to {CLUSTER_TABLE}...")
    output_df = job_skill_groups[['job_id', 'cluster_id']].copy()
    output_df['model_version'] = MODEL_VERSION
    output_df['processed_at'] = datetime.utcnow()
    
    # Configure the load job
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE", # Overwrite table each time
        schema=[
            bigquery.SchemaField("job_id", "STRING"),
            bigquery.SchemaField("cluster_id", "INTEGER"),
            bigquery.SchemaField("model_version", "STRING"),
            bigquery.SchemaField("processed_at", "TIMESTAMP"),
        ]
    )
    
    try:
        load_job = bq_client.load_table_from_dataframe(
            output_df, CLUSTER_TABLE, job_config=job_config
        )
        load_job.result()  # Wait for the job to complete
    except Exception as e:
        print(f"Error loading data into BigQuery: {e}")
        return (f"BigQuery load error: {e}", 500)

    print(f"Successfully trained and saved {len(output_df)} cluster assignments.")
    return (f"Success: Processed {len(output_df)} jobs. Model artifacts saved to {MODEL_VERSION}.", 200)