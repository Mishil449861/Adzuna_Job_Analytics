import json
import functions_framework
import pandas as pd
import numpy as np
import re
import pickle
from datetime import datetime

from google.cloud import bigquery, storage
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score


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
    text = re.sub(r"\s+", " ", text).strip()
    return text


def extract_top_terms(tfidf_matrix, terms, labels, k=10):
    """Return the top TF-IDF words per cluster."""
    top_terms = {}
    for cluster_id in range(max(labels) + 1):
        cluster_indices = np.where(labels == cluster_id)[0]
        centroid = tfidf_matrix[cluster_indices].mean(axis=0)
        top_idx = np.asarray(centroid).ravel().argsort()[-k:][::-1]
        words = [terms[i] for i in top_idx]
        top_terms[cluster_id] = ", ".join(words)
    return top_terms


# ----------------------------
# CLOUD FUNCTION ENTRYPOINT
# ----------------------------
@functions_framework.http
def train_job_cluster(request):

    print("Starting job clustering model training...")

    bq = bigquery.Client(project=PROJECT_ID)
    storage_client = storage.Client(project=PROJECT_ID)

    # Load job data
    print("Loading data from BigQuery...")
    df_jobs = bq.query(f"""
        SELECT job_id, title AS job_title, description as job_description
        FROM `{PROJECT_ID}.{DATASET}.jobs`
        WHERE description IS NOT NULL
    """).to_dataframe()

    if df_jobs.empty:
        return ("No job data available.", 200)

    print(f"Loaded {len(df_jobs)} rows.")

    # Clean text
    print("Cleaning text...")
    df_jobs["clean_text"] = (
        df_jobs["job_title"].fillna("") + " " +
        df_jobs["job_description"].fillna("")
    ).apply(clean)

    # TF-IDF vectorization
    print("Vectorizing text with TF-IDF...")
    vectorizer = TfidfVectorizer(max_features=5000, stop_words="english")
    X = vectorizer.fit_transform(df_jobs["clean_text"])
    terms = vectorizer.get_feature_names_out()

    # K-Means training
    num_clusters = 10
    print(f"Training KMeans with k={num_clusters}...")
    kmeans = KMeans(n_clusters=num_clusters, random_state=42, n_init="auto")
    labels = kmeans.fit_predict(X)

    silhouette = float(silhouette_score(X, labels))
    print(f"Silhouette Score = {silhouette}")

    model_version = f"kmeans_k{num_clusters}_{datetime.utcnow().strftime('%Y%m%d')}"

    # ----------------------------
    # SAVE job_clusters TABLE
    # ----------------------------
    print("Saving job_clusters...")
    df_clusters = pd.DataFrame({
        "job_id": df_jobs["job_id"],
        "cluster_id": labels,
        "model_version": model_version,
        "processed_at": datetime.utcnow()
    })

    df_clusters.to_gbq(
        destination_table="ba882_jobs.job_clusters",
        project_id=PROJECT_ID,
        if_exists="replace"
    )

    print("Saved job_clusters.")

    # ----------------------------
    # SAVE cluster_registry TABLE
    # ----------------------------
    print("Extracting top TF-IDF terms for registry...")
    cluster_terms = extract_top_terms(X, terms, labels)

    df_registry = pd.DataFrame({
        "cluster_id": list(cluster_terms.keys()),
        "cluster_name": [f"Cluster {cid}" for cid in cluster_terms.keys()],
        "top_terms": [cluster_terms[cid] for cid in cluster_terms.keys()],
        "model_version": model_version,
        "updated_at": datetime.utcnow()
    })

    print("Saving cluster_registry...")
    df_registry.to_gbq(
        destination_table="ba882_jobs.cluster_registry",
        project_id=PROJECT_ID,
        if_exists="replace"
    )
    print("Saved cluster_registry.")

    # ----------------------------
    # SAVE model_metrics TABLE
    # ----------------------------
    print("Saving model_metrics...")
    df_metrics = pd.DataFrame([{
        "model_version": model_version,
        "run_date": datetime.utcnow(),
        "silhouette_score": silhouette,
        "method": "kmeans",
        "num_clusters": num_clusters,
        "num_jobs_trained": len(df_jobs)
    }])

    df_metrics.to_gbq(
        destination_table="ba882_jobs.model_metrics",
        project_id=PROJECT_ID,
        if_exists="append"
    )
    print("Saved model_metrics.")

    # ----------------------------
    # SAVE MODEL ARTIFACTS TO GCS
    # ----------------------------
    print("Saving model artifacts to GCS...")
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(f"models/{model_version}/kmeans.pkl")
    blob.upload_from_string(pickle.dumps(kmeans))
    print(f"Saved model under models/{model_version}/kmeans.pkl")

    return (
        f"Success: Processed {len(df_jobs)} jobs. "
        f"Model version: {model_version}.",
        200
    )