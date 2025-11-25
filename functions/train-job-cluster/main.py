import functions_framework
import pandas as pd
import numpy as np
from datetime import datetime

from google.cloud import bigquery, storage

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import KMeans, AgglomerativeClustering
from sklearn.metrics import silhouette_score
from sklearn.decomposition import TruncatedSVD

# --- CONFIG ---
PROJECT_ID = "ba882-team4-474802"
DATASET_ID = "ba882_jobs"

JOBS_TABLE = f"{PROJECT_ID}.{DATASET_ID}.jobs"
SKILLS_TABLE = f"{PROJECT_ID}.{DATASET_ID}.job_skills"
CLUSTER_TABLE = f"{PROJECT_ID}.{DATASET_ID}.job_clusters"
REGISTRY_TABLE = f"{PROJECT_ID}.{DATASET_ID}.cluster_registry"
METRICS_TABLE = f"{PROJECT_ID}.{DATASET_ID}.model_metrics"

QUALITY_THRESHOLD = 0.03
NUM_CLUSTERS = 8

# -------------------------------------------------------------------
# Helper: Extract top terms per cluster for naming
# -------------------------------------------------------------------
def extract_cluster_terms(tfidf_matrix, labels, vectorizer, top_n=8):
    feature_names = np.array(vectorizer.get_feature_names_out())
    clusters = {}

    for cluster_id in range(NUM_CLUSTERS):
        mask = labels == cluster_id
        if mask.sum() == 0:
            clusters[cluster_id] = "Undefined"
            continue

        centroid = tfidf_matrix[mask].mean(axis=0)
        centroid = np.asarray(centroid).ravel()

        top_indices = centroid.argsort()[::-1][:top_n]
        top_terms = feature_names[top_indices]

        clusters[cluster_id] = ", ".join(top_terms)

    return clusters


# -------------------------------------------------------------------
# MAIN FUNCTION
# -------------------------------------------------------------------
@functions_framework.http
def train_cluster_model(request):
    run_id = datetime.utcnow().strftime("%Y%m%d-%H%M")

    bq = bigquery.Client(project=PROJECT_ID)
    storage = storage.Client(project=PROJECT_ID)

    # -------------------------------------------------------------------
    # 1. LOAD DATA
    # -------------------------------------------------------------------
    try:
        jobs_df = bq.query(
            f"SELECT job_id, title FROM `{JOBS_TABLE}`"
        ).to_dataframe()

        skills_df = bq.query(
            f"SELECT source_job_id, skill_name FROM `{SKILLS_TABLE}`"
        ).to_dataframe()

    except Exception as e:
        return (f"Load Error: {e}", 500)

    merged = jobs_df.merge(skills_df, left_on="job_id", right_on="source_job_id")
    grouped = merged.groupby(["job_id", "title"])["skill_name"].apply(list).reset_index()

    # Process text
    processed_text = [
        " ".join(row.skill_name).lower() + " " + row.title.lower()
        for row in grouped.itertuples()
    ]

    # -------------------------------------------------------------------
    # 2. TF-IDF
    # -------------------------------------------------------------------
    vectorizer = TfidfVectorizer(
        max_features=1000, stop_words="english", min_df=0.01, max_df=0.85
    )
    tfidf_matrix = vectorizer.fit_transform(processed_text)

    # -------------------------------------------------------------------
    # 3. TRAIN MODELS
    # -------------------------------------------------------------------
    # A. KMeans
    kmeans = KMeans(n_clusters=NUM_CLUSTERS, n_init=10, random_state=42)
    labels_kmeans = kmeans.fit_predict(tfidf_matrix)
    score_kmeans = silhouette_score(tfidf_matrix, labels_kmeans, sample_size=5000)

    # B. Hierarchical (SVD-reduced)
    svd = TruncatedSVD(n_components=40, random_state=42)
    reduced = svd.fit_transform(tfidf_matrix)

    if len(reduced) <= 5000:
        hier = AgglomerativeClustering(n_clusters=NUM_CLUSTERS)
        labels_hier = hier.fit_predict(reduced)
        score_hier = silhouette_score(reduced, labels_hier)
    else:
        score_hier = -1
        labels_hier = None

    # -------------------------------------------------------------------
    # 4. SELECT WINNER
    # -------------------------------------------------------------------
    if score_hier > score_kmeans:
        winner_name = "Hierarchical"
        winner_score = score_hier
        winner_labels = labels_hier
    else:
        winner_name = "KMeans"
        winner_score = score_kmeans
        winner_labels = labels_kmeans

    MODEL_VERSION = f"{winner_name}_v_{run_id}"

    if winner_score < QUALITY_THRESHOLD:
        print("WARNING: Low silhouette score.")

    grouped["cluster_id"] = winner_labels
    grouped["model_version"] = MODEL_VERSION
    grouped["processed_at"] = datetime.utcnow()

    num_jobs_trained = len(grouped)
    num_clusters = NUM_CLUSTERS

    # -------------------------------------------------------------------
    # 5. EXTRACT CLUSTER LABELS (TOP TERMS)
    # -------------------------------------------------------------------
    cluster_terms = extract_cluster_terms(tfidf_matrix, winner_labels, vectorizer)

    registry_rows = [
        {
            "cluster_id": cid,
            "cluster_name": f"{winner_name} Cluster {cid}",
            "top_terms": terms,
            "model_version": MODEL_VERSION,
            "updated_at": datetime.utcnow().isoformat()
        }
        for cid, terms in cluster_terms.items()
    ]

    bq.load_table_from_json(registry_rows, REGISTRY_TABLE).result()

    # -------------------------------------------------------------------
    # 6. WRITE CLUSTER ASSIGNMENTS
    # -------------------------------------------------------------------
    job_cfg = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    bq.load_table_from_dataframe(grouped, CLUSTER_TABLE, job_cfg).result()

    # -------------------------------------------------------------------
    # 7. WRITE METRICS
    # -------------------------------------------------------------------
    metric_row = [{
        "model_version": MODEL_VERSION,
        "run_date": datetime.utcnow().isoformat(),
        "silhouette_score": float(winner_score),
        "method": winner_name,
        "num_clusters": int(num_clusters),
        "num_jobs_trained": int(num_jobs_trained)
    }]

    bq.load_table_from_json(metric_row, METRICS_TABLE).result()

    return (f"Done. Winner: {winner_name} (Score={winner_score:.4f})", 200)