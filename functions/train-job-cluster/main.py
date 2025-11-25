import functions_framework
import pandas as pd
import joblib
import re
from datetime import datetime
from google.cloud import bigquery, storage
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import KMeans

# --- Configuration ---
PROJECT_ID = "ba882-team4-474802"
DATASET_ID = "ba882_jobs"
JOBS_TABLE = f"{PROJECT_ID}.{DATASET_ID}.jobs"
SKILLS_TABLE = f"{PROJECT_ID}.{DATASET_ID}.job_skills"
CLUSTER_TABLE = f"{PROJECT_ID}.{DATASET_ID}.job_clusters"
REGISTRY_TABLE = f"{PROJECT_ID}.{DATASET_ID}.cluster_registry" # NEW: Stores cluster names
GCS_BUCKET = "adzuna-bucket"
MODEL_VERSION = f"tfidf_k8_{datetime.now().strftime('%Y%m%d')}"

# --- Queries ---
# We need the Title now to determine Seniority
JOBS_QUERY = f"SELECT job_id, title FROM `{JOBS_TABLE}`"
SKILLS_QUERY = f"SELECT source_job_id, skill_name FROM `{SKILLS_TABLE}`"

# --- Helper Functions ---
def extract_seniority(title):
    """Simple rule-based extraction for seniority."""
    t = str(title).lower()
    if 'senior' in t or 'lead' in t or 'principal' in t or 'sr.' in t or 'manager' in t:
        return 'Senior'
    elif 'junior' in t or 'entry' in t or 'intern' in t or 'jr.' in t:
        return 'Junior'
    return 'Mid-Level' # Default

@functions_framework.http
def train_cluster_model(request):
    """
    HTTP-triggered Cloud Function to:
    1. Load data from BigQuery
    2. Extract Seniority & Skills
    3. Train TF-IDF + K-Means model
    4. Auto-Label Clusters
    5. Save results back to BigQuery & GCS
    """
    print("Starting job clustering model training...")
    
    # Initialize Clients
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

    # 2. Process Data & Merge
    print("Processing data...")
    # Merge skills into jobs
    jobs_merged = jobs_df.merge(
        skills_df,
        left_on="job_id",
        right_on="source_job_id",
        how="inner" # Keep only jobs with skills
    )

    # Group skills by job: Result is one row per job with a list of skills
    job_group = jobs_merged.groupby(['job_id', 'title'])['skill_name'].apply(lambda x: list(set(x))).reset_index()
    
    if job_group.empty:
        return ("No jobs with skills found to cluster.", 200)

    # 3. Feature Engineering: Inject Seniority Tags
    processed_text = []
    seniority_levels = []
    
    for _, row in job_group.iterrows():
        # Get seniority
        level = extract_seniority(row['title'])
        seniority_levels.append(level)
        
        # Create text for model: "python sql spark tag_senior"
        skills_str = " ".join(row['skill_name'])
        rich_text = f"{skills_str} tag_{level.lower()}"
        processed_text.append(rich_text)
        
    job_group['seniority'] = seniority_levels

    # 4. Train Model (TF-IDF + K-Means)
    print("Training TF-IDF and K-Means...")
    
    # TF-IDF Vectorizer
    vectorizer = TfidfVectorizer(max_features=1000, stop_words='english', min_df=0.01, max_df=0.85)
    tfidf_matrix = vectorizer.fit_transform(processed_text)
    
    # K-Means
    NUM_CLUSTERS = 8
    kmeans = KMeans(n_clusters=NUM_CLUSTERS, random_state=42, n_init=10)
    job_group['cluster_id'] = kmeans.fit_predict(tfidf_matrix)

    # 5. Interpretability: Name the Clusters
    print("Generating cluster labels...")
    common_words = vectorizer.get_feature_names_out()
    centroids = kmeans.cluster_centers_
    registry_rows = []
    
    for i in range(NUM_CLUSTERS):
        # Find top 5 terms for this cluster center
        top_indices = centroids[i].argsort()[-5:][::-1]
        top_terms = [common_words[ind] for ind in top_indices]
        terms_str = ", ".join(top_terms)
        
        # Naming Logic
        name = "General Data Role"
        if "tag_senior" in terms_str: name = "Senior "
        elif "tag_junior" in terms_str: name = "Junior "
        else: name = ""
            
        if "learning" in terms_str or "torch" in terms_str or "model" in terms_str:
            name += "ML Engineer"
        elif "tableau" in terms_str or "visualization" in terms_str or "dashboard" in terms_str:
            name += "Data Analyst"
        elif "spark" in terms_str or "hadoop" in terms_str or "pipeline" in terms_str:
            name += "Data Engineer"
        else:
            name += "Tech Role"

        registry_rows.append({
            "cluster_id": i,
            "cluster_name": name.strip(),
            "top_terms": terms_str,
            "updated_at": datetime.utcnow()
        })

    # 6. Save Artifacts to GCS
    print(f"Saving artifacts to GCS bucket {GCS_BUCKET}...")
    bucket = storage_client.bucket(GCS_BUCKET)
    
    blob_vect = bucket.blob(f"ml_artifacts/{MODEL_VERSION}/tfidf_vectorizer.pkl")
    with blob_vect.open("wb") as f:
        joblib.dump(vectorizer, f)
        
    blob_model = bucket.blob(f"ml_artifacts/{MODEL_VERSION}/kmeans_model.pkl")
    with blob_model.open("wb") as f:
        joblib.dump(kmeans, f)

    # 7. Save Results to BigQuery
    # A. Save Assignments (Job -> Cluster)
    print(f"Saving assignments to {CLUSTER_TABLE}...")
    output_df = job_group[['job_id', 'cluster_id', 'seniority']].copy()
    output_df['processed_at'] = datetime.utcnow()
    
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    job = bq_client.load_table_from_dataframe(output_df, CLUSTER_TABLE, job_config=job_config)
    job.result()
    
    # B. Save Registry (Cluster ID -> Name)
    print(f"Saving registry to {REGISTRY_TABLE}...")
    registry_df = pd.DataFrame(registry_rows)
    job_reg = bq_client.load_table_from_dataframe(registry_df, REGISTRY_TABLE, job_config=job_config)
    job_reg.result()

    return (f"Success: Processed {len(output_df)} jobs into {NUM_CLUSTERS} clusters.", 200)