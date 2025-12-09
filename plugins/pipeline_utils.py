import io
import logging
import requests
import pandas as pd
from datetime import datetime
from airflow.providers.google.cloud.hooks.gcs import GCSHook

# --- Adzuna API Configuration ---
COUNTRY = "us"
BASE_URL = f"https://api.adzuna.com/v1/api/jobs/{COUNTRY}/search"

def fetch_data(app_id, app_key, pages=5, per_page=50):
    """Fetch job listings from Adzuna API."""
    all_results = []
    for page in range(1, pages + 1):
        url = (
            f"{BASE_URL}/{page}"
            f"?app_id={app_id}"
            f"&app_key={app_key}"
            f"&results_per_page={per_page}"
            f"&what=data"
        )
        # --- THIS IS CODE-LEVEL LOGGING ---
        logging.info(f"Fetching page {page}")
        response = requests.get(url)

        if response.status_code != 200:
            # --- THIS IS CODE-LEVEL LOGGING ---
            logging.error(f"Failed to fetch page {page}: {response.text[:300]}")
            continue
        try:
            results = response.json().get("results", [])
            all_results.extend(results)
        except Exception as e:
            logging.error(f"Error parsing page {page}: {e}")

    logging.info(f"Fetched {len(all_results)} total jobs")
    return all_results

def transform_data(records, ingest_timestamp):
    """
    Transform raw Adzuna data into a star schema based on the relationship table.
    Creates 3 dimension tables (Companies, Locations, Categories)
    and 2 fact tables (Jobs, JobStats).
    """
    jobs_rows, jobstats_rows = [], []
    companies_set, locations_set, categories_set = set(), set(), set()
    ingest_ts_str = ingest_timestamp.isoformat()

    for r in records:
        jid = r.get("id")
        created_str = r.get("created")
        
        # --- Extract Foreign Keys ---
        company_name = r.get("company", {}).get("display_name")
        category_label = r.get("category", {}).get("label")
        
        # Location logic
        loc = r.get("location", {})
        area = loc.get("area", [])
        city, state, country = None, None, None
        if isinstance(area, list) and len(area) > 0:
            country = area[0]
            if len(area) > 1:
                state = area[1]
            if len(area) > 2:
                city = area[2]
        
        # --- Add to Sets (for unique dimension rows) ---
        if company_name:
            companies_set.add(company_name)
        if category_label:
            categories_set.add(category_label)
        if city or state:
            locations_set.add((city, state, country))
            
        # --- Parse Timestamps / Other Logic ---
        try:
            created_ts = pd.to_datetime(created_str)
        except:
            created_ts = None
            
        posting_week = None
        if created_ts:
            try:
                posting_week = str(created_ts.isocalendar().week)
            except Exception:
                pass

        # 1. JOBS Table Row (with Foreign Keys)
        jobs_rows.append({
            "job_id": jid,
            "title": r.get("title"),
            "description": r.get("description"),
            "salary_min": r.get("salary_min"),
            "salary_max": r.get("salary_max"),
            "created": created_ts,
            "redirected_url": r.get("redirect_url"),
            "ingest_data": ingest_ts_str,
            "ingest_ts": ingest_timestamp,
            "company_name": company_name,
            "category_label": category_label,
            "city": city,
            "state": state
        })
        
        # 2. JOBSTATS Table Row
        jobstats_rows.append({
            "job_id": jid,
            "contract_type": r.get("contract_type"),
            "contract_time": r.get("contract_time"),
            "posting_week": posting_week,
        })

    # --- Create DataFrames ---
    jobs_df = pd.DataFrame(jobs_rows)
    jobstats_df = pd.DataFrame(jobstats_rows)
    companies_df = pd.DataFrame([{"company_name": c} for c in companies_set])
    locations_df = pd.DataFrame(
        [{"city": l[0], "state": l[1], "country": l[2]} for l in locations_set]
    )
    categories_df = pd.DataFrame([{"category_label": c} for c in categories_set])

    dfs = {
        "jobs": jobs_df,
        "jobstats": jobstats_df,
        "companies": companies_df,
        "locations": locations_df,
        "categories": categories_df,
    }
    
    # --- THIS IS CODE-LEVEL LOGGING ---
    logging.info("DataFrame counts: " + " | ".join([f"{k}: {len(v)}" for k, v in dfs.items()]))
    return dfs

def upload_dfs_to_gcs(dfs, bucket_name, ingest_timestamp, gcp_conn_id):
    """Uploads multiple DataFrames to GCS as Parquet."""
    gcs_hook = GCSHook(gcp_conn_id=gcp_conn_id)
    ts_path = ingest_timestamp.strftime("%Y%m%d%H%M%S")
    gcs_paths = {}

    for name, df in dfs.items():
        if df.empty:
            logging.warning(f"Skipping upload for {name}: empty DataFrame")
            continue
        
        gcs_path = f"processed/{name}/{name}_{ts_path}.parquet"
        
        try:
            parquet_buffer = io.BytesIO()
            df.to_parquet(parquet_buffer, index=False, engine="pyarrow")
            parquet_buffer.seek(0)
            
            gcs_hook.upload(
                bucket_name=bucket_name,
                object_name=gcs_path,
                data=parquet_buffer.read(),
                mime_type="application/octet-stream",
            )
            
            gs_path_full = f"gs://{bucket_name}/{gcs_path}"
            logging.info(f"✅ Uploaded {name} -> {gs_path_full}")
            gcs_paths[name] = gcs_path
        except Exception as e:
            logging.error(f"Failed to upload {name}: {e}")
            raise
            
    return gcs_paths