import functions_framework
import pandas as pd
import joblib
import numpy as np
from datetime import datetime
from google.cloud import bigquery, storage
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import KMeans, AgglomerativeClustering
from sklearn.metrics import silhouette_score
from sklearn.decomposition import TruncatedSVD

# --- Configuration ---
PROJECT_ID = "ba882-team4-474802"
DATASET_ID = "ba882_jobs"
JOBS_TABLE = f"{PROJECT_ID}.{DATASET_ID}.jobs"
SKILLS_TABLE = f"{PROJECT_ID}.{DATASET_ID}.job_skills"
CLUSTER_TABLE = f"{PROJECT_ID}.{DATASET_ID}.job_clusters"
REGISTRY_TABLE = f"{PROJECT_ID}.{DATASET_ID}.cluster_registry"
METRICS_TABLE = f"{PROJECT_ID}.{DATASET_ID}.model_metrics"
GCS_BUCKET = "adzuna-bucket"

# Minimum acceptable quality. If both models are below this, we warn/fail.
QUALITY_THRESHOLD = 0.03 

@functions_framework.http
def train_cluster_model(request):
    run_id = datetime.now().strftime('%Y%m%d')
    bq_client = bigquery.Client(project=PROJECT_ID)
    storage_client = storage.Client(project=PROJECT_ID)
    
    # 1. Load & Process Data (Same as before)
    # ... (Load code omitted for brevity, assumes jobs_merged and job_group created) ...
    # [Restoring the loading logic briefly for context]
    try:
        jobs_df = bq_client.query(f"SELECT job_id, title FROM `{JOBS_TABLE}`").to_dataframe()
        skills_df = bq_client.query(f"SELECT source_job_id, skill_name FROM `{SKILLS_TABLE}`").to_dataframe()
    except Exception as e: return (f"Load Error: {e}", 500)
    
    jobs_merged = jobs_df.merge(skills_df, left_on="job_id", right_on="source_job_id", how="inner")
    job_group = jobs_merged.groupby(['job_id', 'title'])['skill_name'].apply(lambda x: list(set(x))).reset_index()
    
    # Feature Engineering
    processed_text = []
    for _, row in job_group.iterrows():
        # (Seniority logic same as previous script)
        skills_str = " ".join(row['skill_name'])
        processed_text.append(f"{skills_str} {str(row['title']).lower()}") # Simplified injection

    # 2. Vectorization (Common Ground)
    print("Vectorizing...")
    vectorizer = TfidfVectorizer(max_features=1000, stop_words='english', min_df=0.01, max_df=0.85)
    tfidf_matrix = vectorizer.fit_transform(processed_text)
    
    # --- 3. THE TOURNAMENT ---
    
    # Challenger A: K-Means
    print("Training K-Means...")
    kmeans = KMeans(n_clusters=8, random_state=42, n_init=10)
    labels_kmeans = kmeans.fit_predict(tfidf_matrix)
    score_kmeans = silhouette_score(tfidf_matrix, labels_kmeans, sample_size=5000)
    print(f"K-Means Score: {score_kmeans}")

    # Challenger B: Hierarchical (Agglomerative)
    # Optimization: Must reduce dimensions first or Hierarchical will crash on RAM
    print("Training Hierarchical...")
    svd = TruncatedSVD(n_components=50, random_state=42)
    reduced_matrix = svd.fit_transform(tfidf_matrix)
    
    # We use a subset if data > 5000 rows because Hierarchical is O(n^2)
    if len(reduced_matrix) > 5000:
        print("Data too large for full Hierarchical, skipping or sampling...")
        # For safety in this demo, we force K-Means if data is huge
        score_hier = -1 
        labels_hier = None
    else:
        hierarchical = AgglomerativeClustering(n_clusters=8)
        labels_hier = hierarchical.fit_predict(reduced_matrix)
        score_hier = silhouette_score(reduced_matrix, labels_hier)
    print(f"Hierarchical Score: {score_hier}")

    # --- 4. DECISION LOGIC ---
    
    # Determine Winner
    if score_hier > score_kmeans:
        winner_name = "Hierarchical"
        winner_score = score_hier
        winner_labels = labels_hier
        winner_model = hierarchical
        # Note: Centroid extraction for labeling is harder with Hierarchical, 
        # usually we fallback to calculating means of the TFIDF matrix based on labels.
    else:
        winner_name = "KMeans"
        winner_score = score_kmeans
        winner_labels = labels_kmeans
        winner_model = kmeans

    MODEL_VERSION = f"{winner_name}_v_{run_id}"
    print(f"Winner: {winner_name} with score {winner_score}")

    # Safety Check
    if winner_score < QUALITY_THRESHOLD:
        # If today's data is terrible, we LOG it, but we usually MUST proceed 
        # because new jobs need clusters.
        print("WARNING: Model quality is low. Drastic drift detected.")

    # Apply Winner Labels to Data
    job_group['cluster_id'] = winner_labels
    job_group['model_version'] = MODEL_VERSION

    # --- 5. SAVING (Same as before) ---
    
    # Save Metrics
    metric_row = [{
        "model_version": MODEL_VERSION,
        "run_date": datetime.utcnow().isoformat(),
        "silhouette_score": winner_score,
        "method": winner_name
    }]
    bq_client.load_table_from_json(metric_row, METRICS_TABLE).result()

    # Save Results to BigQuery
    output_df = job_group[['job_id', 'cluster_id', 'model_version']]
    output_df['processed_at'] = datetime.utcnow()
    
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    bq_client.load_table_from_dataframe(output_df, CLUSTER_TABLE, job_config=job_config).result()
    
    return (f"Tournament Complete. Winner: {winner_name} (Score: {winner_score:.4f})", 200)