import functions_framework
import pandas as pd
import numpy as np
import re
import json
import logging
from datetime import datetime
from google.cloud import bigquery, storage
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score, pairwise_distances_argmin_min
from sklearn.preprocessing import OneHotEncoder
from sklearn.feature_extraction.text import TfidfVectorizer
import openai

# --- CONFIG ---
PROJECT_ID = "ba882-team4-474802"
DATASET = "ba882_jobs"
BUCKET_NAME = "adzuna-bucket"
OPENAI_API_KEY = "YOUR_OPENAI_KEY" 

# --- HELPER: TEXT CLEANING ---
def clean_text(text):
    if not isinstance(text, str): return ""
    text = re.sub(r"[^\w\s]", " ", text.lower())
    return re.sub(r"\s+", " ", text).strip()

# --- HELPER: EMBEDDINGS ---
def get_embeddings_batch(texts):
    client = openai.OpenAI(api_key=OPENAI_API_KEY)
    response = client.embeddings.create(input=texts, model="text-embedding-3-large")
    return np.array([d.embedding for d in response.data])

# --- HELPER: CLUSTER NAMING (IMPROVED) ---
def name_cluster(cluster_keywords, sample_texts):
    client = openai.OpenAI(api_key=OPENAI_API_KEY)
    
    # improved prompt: explicitly forbids generic names
    prompt = f"""
    You are a specialized Data Job Taxonomist. 
    Name this job cluster based on its distinct keywords and representative job descriptions.

    DISTINCT KEYWORDS (Unique to this group): {", ".join(cluster_keywords)}
    
    REPRESENTATIVE JOB SAMPLES:
    {json.dumps(sample_texts, indent=2)}

    RULES:
    1. Name MUST be specific (e.g., "Clinical Data Management" NOT "Data Management").
    2. Do NOT use the words "Cluster", "Group", "Generic", or "Professional".
    3. Avoid repetitive prefixes like "Data Engineering..." if a more specific term exists (e.g., "ETL Pipeline Development").
    4. Max 4 words.

    OUTPUT: Just the name.
    """
    try:
        resp = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.3 # Lower temp for more deterministic/factual naming
        )
        return resp.choices[0].message.content.strip().replace('"', '')
    except Exception as e:
        logging.error(f"LLM Naming failed: {e}")
        return f"Cluster_{cluster_keywords[0]}_{cluster_keywords[1]}"

@functions_framework.http
def train_job_cluster(request):
    logging.info("Starting Intelligent Hybrid Clustering Job...")
    
    bq = bigquery.Client(project=PROJECT_ID)
    
    # 1. Fetch Data
    query = f"""
        SELECT job_id, title, description, category_label, company_name, salary_min, salary_max
        FROM `{PROJECT_ID}.{DATASET}.jobs`
        WHERE description IS NOT NULL
        LIMIT 3000
    """
    df = bq.query(query).to_dataframe()
    if df.empty: return json.dumps({"status": "error"}), 400

    # 2. Preprocess
    df['clean_text'] = (df['title'] + " " + df['description']).apply(clean_text)
    df['category_label'] = df['category_label'].fillna('Unknown')

    # 3. Generate Features
    # Text Embeddings
    embeddings = get_embeddings_batch(df['clean_text'].tolist())
    
    # Categorical Features
    encoder = OneHotEncoder(sparse_output=False)
    cat_features = encoder.fit_transform(df[['category_label', 'company_name']])
    
    # TF-IDF (For keyword extraction only, not clustering)
    tfidf = TfidfVectorizer(max_features=500, stop_words='english')
    tfidf_matrix = tfidf.fit_transform(df['clean_text'])
    feature_names = np.array(tfidf.get_feature_names_out())

    # 4. Run Hybrid Clustering
    kmeans_emb = KMeans(n_clusters=10, random_state=42).fit(embeddings)
    score_emb = silhouette_score(embeddings, kmeans_emb.labels_)
    
    kmeans_cat = KMeans(n_clusters=10, random_state=42).fit(cat_features)
    score_cat = silhouette_score(cat_features, kmeans_cat.labels_)

    # 5. Select Winner
    if score_emb >= score_cat:
        winner = "Embeddings"
        model = kmeans_emb
        data_matrix = embeddings
        final_labels = kmeans_emb.labels_
        final_score = score_emb
    else:
        winner = "Categorical"
        model = kmeans_cat
        data_matrix = cat_features
        final_labels = kmeans_cat.labels_
        final_score = score_cat

    # 6. Intelligent Naming Strategy
    df['cluster_id'] = final_labels
    cluster_registry = []

    for cid in range(10):
        # A. Get Representative Samples (Closest to Centroid)
        # Find indices of points in this cluster
        cluster_indices = np.where(final_labels == cid)[0]
        
        if len(cluster_indices) > 0:
            # Get the center of this specific cluster
            center = model.cluster_centers_[cid]
            # Find the 5 jobs closest to the center (The "Archetypes")
            closest, _ = pairwise_distances_argmin_min([center], data_matrix[cluster_indices])
            # Map back to original dataframe indices
            representative_indices = cluster_indices[closest] # Note: simplistic approx for single center
            # Better approach for batch: sort by distance
            distances = np.linalg.norm(data_matrix[cluster_indices] - center, axis=1)
            sorted_idx = np.argsort(distances)[:5]
            best_idx = cluster_indices[sorted_idx]
            
            samples = df.iloc[best_idx]['title'].tolist() # Use Title + Desc for LLM
            
            # B. Get Distinctive Keywords (TF-IDF)
            # Get average TF-IDF vector for this cluster
            avg_tfidf = np.asarray(tfidf_matrix[cluster_indices].mean(axis=0)).flatten()
            # Get top 5 words with highest score in this cluster
            top_kw_idx = avg_tfidf.argsort()[-5:][::-1]
            keywords = feature_names[top_kw_idx].tolist()
            
            # C. Generate Name
            c_name = name_cluster(keywords, samples)
        else:
            c_name = "Unpopulated Cluster"
            keywords = []

        cluster_registry.append({
            "cluster_id": cid,
            "cluster_name": c_name,
            "top_terms": ", ".join(keywords), # Saving keywords for debugging
            "job_count": int(len(cluster_indices)),
            "avg_salary_min": float(df[df['cluster_id'] == cid]['salary_min'].mean()),
            "avg_salary_max": float(df[df['cluster_id'] == cid]['salary_max'].mean()),
            "model_version": f"{winner}_{datetime.now().strftime('%Y%m%d')}",
            "updated_at": datetime.utcnow().isoformat()
        })

    # 7. Save to BigQuery
    job_clusters_df = df[['job_id', 'cluster_id']].copy()
    job_clusters_df['model_version'] = f"{winner}_{datetime.now().strftime('%Y%m%d')}"
    job_clusters_df['processed_at'] = datetime.utcnow()
    
    job_clusters_df.to_gbq(f"{DATASET}.job_clusters", project_id=PROJECT_ID, if_exists='replace')
    pd.DataFrame(cluster_registry).to_gbq(f"{DATASET}.cluster_registry", project_id=PROJECT_ID, if_exists='replace')

    return json.dumps({"status": "success", "winner": winner, "score": final_score}), 200