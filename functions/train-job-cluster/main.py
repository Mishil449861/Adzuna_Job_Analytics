import functions_framework
import pandas as pd
import numpy as np
import pickle
import re
import json
import logging
from datetime import datetime
from google.cloud import bigquery, storage
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
from sklearn.preprocessing import OneHotEncoder
import openai

# --- CONFIG ---
PROJECT_ID = "ba882-team4-474802"
DATASET = "ba882_jobs"
BUCKET_NAME = "adzuna-bucket"
OPENAI_API_KEY = "YOUR_OPENAI_KEY"  # Recommend using Secret Manager in prod

# --- HELPER: TEXT CLEANING ---
def clean_text(text):
    if not isinstance(text, str): return ""
    text = re.sub(r"[^\w\s]", " ", text.lower())
    return re.sub(r"\s+", " ", text).strip()

# --- HELPER: EMBEDDINGS ---
def get_embeddings_batch(texts):
    """Generates embeddings using OpenAI (or swap for Gemini)."""
    client = openai.OpenAI(api_key=OPENAI_API_KEY)
    # OpenAI handles batching, but for huge datasets, chunk this loop
    response = client.embeddings.create(input=texts, model="text-embedding-3-large")
    return np.array([d.embedding for d in response.data])

# --- HELPER: CLUSTER NAMING ---
def name_cluster(sample_texts):
    client = openai.OpenAI(api_key=OPENAI_API_KEY)
    prompt = f"""
    Analyze these job descriptions from a cluster:
    {sample_texts[:5]}
    
    Provide a concise 3-5 word name for this job cluster.
    Examples: "Data Engineering", "Nursing Staff", "Retail Management".
    Do not use "Cluster" in the name.
    """
    resp = client.chat.completions.create(
        model="gpt-4o",
        messages=[{"role": "user", "content": prompt}]
    )
    return resp.choices[0].message.content.strip()

@functions_framework.http
def train_job_cluster(request):
    logging.info("Starting Hybrid Clustering Job...")
    
    bq = bigquery.Client(project=PROJECT_ID)
    storage_client = storage.Client(project=PROJECT_ID)

    # 1. Fetch Data (Text + Categorical Features)
    query = f"""
        SELECT 
            job_id, title, description, 
            category_label, company_name, 
            salary_min, salary_max
        FROM `{PROJECT_ID}.{DATASET}.jobs`
        WHERE description IS NOT NULL
        LIMIT 2000 -- Adjust based on memory limits of Cloud Function
    """
    df = bq.query(query).to_dataframe()
    
    if df.empty:
        return json.dumps({"status": "error", "message": "No data found"}), 400

    # 2. Preprocess
    df['clean_text'] = (df['title'] + " " + df['description']).apply(clean_text)
    df['category_label'] = df['category_label'].fillna('Unknown')
    
    # --- METHOD A: EMBEDDING CLUSTERING ---
    logging.info("Running Method A: Embeddings...")
    embeddings = get_embeddings_batch(df['clean_text'].tolist())
    
    kmeans_emb = KMeans(n_clusters=5, random_state=42)
    labels_emb = kmeans_emb.fit_predict(embeddings)
    score_emb = silhouette_score(embeddings, labels_emb)
    logging.info(f"Embedding Silhouette Score: {score_emb}")

    # --- METHOD B: CATEGORICAL CLUSTERING ---
    logging.info("Running Method B: Categorical...")
    encoder = OneHotEncoder(sparse_output=False)
    # Using Category + Company as features
    cat_features = encoder.fit_transform(df[['category_label', 'company_name']])
    
    kmeans_cat = KMeans(n_clusters=5, random_state=42)
    labels_cat = kmeans_cat.fit_predict(cat_features)
    # Calculate silhouette on categorical features
    score_cat = silhouette_score(cat_features, labels_cat)
    logging.info(f"Categorical Silhouette Score: {score_cat}")

    # --- WINNER SELECTION ---
    if score_emb >= score_cat:
        winner = "Embeddings"
        final_labels = labels_emb
        final_score = score_emb
    else:
        winner = "Categorical"
        final_labels = labels_cat
        final_score = score_cat
    
    logging.info(f"🏆 Winner: {winner} (Score: {final_score:.4f})")

    # --- ASSIGN LABELS & NAME CLUSTERS ---
    df['cluster_id'] = final_labels
    cluster_registry = []
    
    for cid in range(10):
        # Get samples from this cluster
        samples = df[df['cluster_id'] == cid]['clean_text'].head(5).tolist()
        if samples:
            c_name = name_cluster(samples)
        else:
            c_name = "Uncategorized"
            
        cluster_registry.append({
            "cluster_id": cid,
            "cluster_name": c_name,
            "job_count": int(len(df[df['cluster_id'] == cid])),
            "avg_salary_min": float(df[df['cluster_id'] == cid]['salary_min'].mean()),
            "avg_salary_max": float(df[df['cluster_id'] == cid]['salary_max'].mean()),
            "model_version": f"{winner}_{datetime.now().strftime('%Y%m%d')}",
            "updated_at": datetime.utcnow().isoformat()
        })

    # --- SAVE TO BIGQUERY ---
    # 1. Job Assignments
    job_clusters_df = df[['job_id', 'cluster_id']].copy()
    job_clusters_df['model_version'] = f"{winner}_{datetime.now().strftime('%Y%m%d')}"
    job_clusters_df['processed_at'] = datetime.utcnow()
    
    job_clusters_df.to_gbq(f"{DATASET}.job_clusters", project_id=PROJECT_ID, if_exists='replace')
    
    # 2. Registry
    registry_df = pd.DataFrame(cluster_registry)
    registry_df.to_gbq(f"{DATASET}.cluster_registry", project_id=PROJECT_ID, if_exists='replace')

    return json.dumps({
        "status": "success", 
        "winner": winner, 
        "score": final_score,
        "clusters_created": 5
    }), 200