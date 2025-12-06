# train_job_cluster.py
import json
import functions_framework
import pandas as pd
import numpy as np
import pickle
import re
from datetime import datetime

from google.cloud import bigquery, storage
from flask import jsonify
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
from sklearn.preprocessing import OneHotEncoder

# Ensure you have this utility or replace with your embedding logic
from utils_embeddings import get_text_embedding

PROJECT_ID = "ba882-team4-474802"
DATASET = "ba882_jobs"
BUCKET_NAME = "adzuna-bucket"

# ----------------------------
# Helpers
# ----------------------------
def clean(text):
    if not isinstance(text, str):
        return ""
    text = text.lower()
    text = re.sub(r"[^\w\s]", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()

def extract_seniority(text):
    if not isinstance(text, str):
        return "none"
    t = text.lower()

    if re.search(r"\b(entry[-\s]*level|junior|jr)\b", t):
        return "junior"
    if re.search(r"\b(senior|sr\.)\b", t):
        return "senior"
    if re.search(r"\b(lead|principal)\b", t):
        return "lead"
    if re.search(r"\b(manager|management)\b", t):
        return "manager"
    if re.search(r"\b(mid[-\s]*level|intermediate)\b", t):
        return "mid"
    return "none"

def summarize_cluster_with_llm(samples):
    """
    Take a representative sample of job texts from the cluster
    and generate a clean human-readable cluster name.
    """
    # Import OpenAI inside function to avoid global scope issues in some environments
    try:
        from openai import OpenAI
        client = OpenAI() # Assumes OPENAI_API_KEY is in env vars
        
        prompt = f"""
        You are labeling job clusters.
        Here are sample job titles/descriptions for a cluster:

        {samples}

        Provide ONLY a concise 3–6 word cluster name describing this group.
        """

        completion = client.chat.completions.create(
            model="gpt-4o-mini", # Updated to current cost-effective model
            messages=[{"role": "user", "content": prompt}]
        )
        return completion.choices[0].message.content.strip()
    except Exception as e:
        print(f"LLM Error: {e}")
        return "Unknown Cluster"

# ----------------------------
# Cloud Function Entrypoint
# ----------------------------
@functions_framework.http
def train_job_cluster(request):
    print("Starting hybrid job clustering...")

    bq = bigquery.Client(project=PROJECT_ID)
    storage_client = storage.Client(project=PROJECT_ID)

    # 1. Load Data (Fetched Extra Columns for Categorical Clustering)
    query = f"""
        SELECT 
            job_id, 
            title AS job_title, 
            description AS job_description,
            category_label,
            contract_time,
            company_name
        FROM `{PROJECT_ID}.{DATASET}.jobs`
        WHERE description IS NOT NULL
    """
    df_jobs = bq.query(query).to_dataframe()

    if df_jobs.empty:
        return ("No job data found", 200)

    print(f"Loaded {len(df_jobs)} jobs")

    # 2. Preprocessing
    # A. Text Cleaning
    df_jobs["clean_text"] = (
        df_jobs["job_title"].fillna("") + " " +
        df_jobs["job_description"].fillna("")
    ).apply(clean)

    # B. Feature Extraction
    df_jobs["seniority"] = df_jobs["job_description"].apply(extract_seniority)
    
    # Fill NAs for categorical features
    df_jobs["category_label"] = df_jobs["category_label"].fillna("unknown")
    df_jobs["contract_time"] = df_jobs["contract_time"].fillna("unknown")

    # ----------------------------
    # Method A: Embedding Clustering
    # ----------------------------
    print("--- Method A: Generating Embeddings ---")
    embeddings = np.vstack([get_text_embedding(t) for t in df_jobs["clean_text"]])
    
    num_clusters = 10
    kmeans_embed = KMeans(n_clusters=num_clusters, random_state=42)
    labels_embed = kmeans_embed.fit_predict(embeddings)
    
    score_embed = silhouette_score(embeddings, labels_embed)
    print(f"Method A (Embeddings) Silhouette: {score_embed:.4f}")

    # ----------------------------
    # Method B: Categorical Clustering
    # ----------------------------
    print("--- Method B: Categorical Encoding ---")
    # Using One-Hot Encoding + KMeans (Robust "Categorical" Approach)
    # We choose high-signal columns: Category, Seniority, Contract Type
    cat_features = df_jobs[["category_label", "seniority", "contract_time"]]
    
    encoder = OneHotEncoder(sparse_output=False, handle_unknown='ignore')
    encoded_cats = encoder.fit_transform(cat_features)
    
    kmeans_cat = KMeans(n_clusters=num_clusters, random_state=42)
    labels_cat = kmeans_cat.fit_predict(encoded_cats)
    
    # Calculate score (using Euclidean distance on One-Hot space is valid for separation measurement)
    score_cat = silhouette_score(encoded_cats, labels_cat)
    print(f"Method B (Categorical) Silhouette: {score_cat:.4f}")

    # ----------------------------
    # Selection Logic
    # ----------------------------
    if score_cat > score_embed:
        print(">>> WINNER: Categorical Clustering")
        final_labels = labels_cat
        final_model = kmeans_cat
        final_method = "categorical_kmeans"
        final_score = score_cat
        # Artifact to save is the encoder + model for categorical
        artifact_data = {"model": final_model, "encoder": encoder, "type": "categorical"}
    else:
        print(">>> WINNER: Embedding Clustering")
        final_labels = labels_embed
        final_model = kmeans_embed
        final_method = "embedding_kmeans"
        final_score = score_embed
        artifact_data = {"model": final_model, "type": "embedding"}

    # ----------------------------
    # LLM Naming (Applied to Winner)
    # ----------------------------
    print(f"Naming clusters for method: {final_method}...")
    cluster_names = {}
    
    for cid in range(num_clusters):
        # Find indices belonging to this cluster
        cluster_indices = np.where(final_labels == cid)[0]
        
        if len(cluster_indices) == 0:
            cluster_names[cid] = "Empty Cluster"
            continue

        # Get sample texts from the original dataframe using the winning indices
        # We always use the TEXT for naming, even if categorical won, 
        # because humans read text, not one-hot vectors.
        sample_texts = df_jobs.iloc[cluster_indices]["clean_text"].sample(
            min(5, len(cluster_indices))
        ).tolist()

        cluster_names[cid] = summarize_cluster_with_llm(sample_texts)

    # ----------------------------
    # Save Results
    # ----------------------------
    model_version = f"{final_method}_k{num_clusters}_{datetime.utcnow().strftime('%Y%m%d')}"

    # 1. Job Clusters Table
    df_clusters = pd.DataFrame({
        "job_id": df_jobs["job_id"],
        "cluster_id": final_labels,
        "seniority": df_jobs["seniority"], # Keep mainly for reference
        "model_version": model_version,
        "processed_at": datetime.utcnow()
    })

    df_clusters.to_gbq(
        destination_table=f"{PROJECT_ID}.{DATASET}.job_clusters",
        project_id=PROJECT_ID,
        if_exists="replace"
    )

    # 2. Cluster Registry Table
    df_registry = pd.DataFrame({
        "cluster_id": list(cluster_names.keys()),
        "cluster_name": list(cluster_names.values()),
        "top_terms": list(cluster_names.values()),
        "model_version": model_version,
        "updated_at": datetime.utcnow()
    })

    df_registry.to_gbq(
        destination_table=f"{PROJECT_ID}.{DATASET}.cluster_registry",
        project_id=PROJECT_ID,
        if_exists="replace"
    )

    # 3. Model Metrics
    df_metrics = pd.DataFrame([{
        "model_version": model_version,
        "run_date": datetime.utcnow(),
        "silhouette_score": final_score,
        "method": final_method,
        "num_clusters": num_clusters,
        "num_jobs_trained": len(df_jobs)
    }])

    df_metrics.to_gbq(
        destination_table=f"{PROJECT_ID}.{DATASET}.model_metrics",
        project_id=PROJECT_ID,
        if_exists="append"
    )

    # 4. Save Artifacts
    bucket = storage_client.bucket(BUCKET_NAME)
    blob_path = f"models/{model_version}/model_artifacts.pkl"
    
    bucket.blob(blob_path).upload_from_string(
        pickle.dumps(artifact_data)
    )

    print(f"Success. Winner: {final_method}. Artifacts saved to {blob_path}")

    return jsonify({
        "status": "success",
        "processed_rows": len(df_jobs),
        "model_version": model_version,
        "method_used": final_method,
        "silhouette_score": final_score
    }), 200