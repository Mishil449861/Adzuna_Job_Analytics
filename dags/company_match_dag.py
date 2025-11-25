import pendulum
import re
import pandas as pd
from airflow.decorators import dag, task
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

# --- CONFIGURATION ---
GCP_PROJECT_ID = "ba882-team4-474802"
GCP_DATASET_ID = "ba882_jobs"
GCP_CONN_ID = "gcp_default"

# Table References
TABLE_COMPANIES = f"{GCP_PROJECT_ID}.{GCP_DATASET_ID}.companies"
TABLE_H1B = f"{GCP_PROJECT_ID}.{GCP_DATASET_ID}.company_sponsorship_status"
TABLE_MAPPING = f"{GCP_PROJECT_ID}.{GCP_DATASET_ID}.temp_name_mapping"
# RENAMED as requested
TABLE_FINAL_MATCH = f"{GCP_PROJECT_ID}.{GCP_DATASET_ID}.sponsor_companies"

def normalize_company_name(name):
    """
    Standardizes company names to maximize matches.
    'System One Holdings LLC' -> 'system one'
    """
    if not name:
        return ""
    
    clean = str(name).lower().strip()
    clean = re.sub(r'\([^)]*\)', '', clean) # Remove (text)
    
    # Expanded suffix list to catch more variations
    suffixes = [
        r'\binc\.?\b', r'\bllc\.?\b', r'\bltd\.?\b', r'\bcorp\.?\b', 
        r'\bcorporation\b', r'\bco\.?\b', r'\bcompany\b', r'\bpllc\b', 
        r'\bpbc\b', r'\bgroup\b', r'\bholdings\b', r'\btechnologies\b',
        r'\bsolutions\b', r'\bservices\b', r'\bpartners\b'
    ]
    pattern = '|'.join(suffixes)
    clean = re.sub(pattern, '', clean)
    
    clean = re.sub(r'[^\w\s]', '', clean) # Remove punctuation
    clean = re.sub(r'\s+', ' ', clean).strip() # Remove extra spaces
    
    return clean

@dag(
    dag_id="company_matching_pipeline",
    schedule="@daily",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["h1b", "matching", "bigquery"],
)
def company_matching_pipeline():

    @task
    def build_mapping_table():
        """
        Creates a clean 1-to-1 mapping table between Adzuna Names and H1B Names.
        """
        hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID)
        
        # 1. Get distinct Names (Standard SQL dialect to avoid backtick error)
        df_adzuna = hook.get_pandas_df(
            sql=f"SELECT DISTINCT company_name FROM `{TABLE_COMPANIES}` WHERE company_name IS NOT NULL",
            dialect='standard'
        )
        
        df_h1b = hook.get_pandas_df(
            sql=f"SELECT DISTINCT employer_name FROM `{TABLE_H1B}` WHERE employer_name IS NOT NULL",
            dialect='standard'
        )
        
        print(f"Fetched {len(df_adzuna)} Adzuna names and {len(df_h1b)} H1B names.")

        # 2. Normalize
        df_adzuna['clean_name'] = df_adzuna['company_name'].apply(normalize_company_name)
        df_h1b['clean_name'] = df_h1b['employer_name'].apply(normalize_company_name)

        # 3. Merge on the clean name
        merged_df = pd.merge(
            df_adzuna, 
            df_h1b, 
            on='clean_name', 
            how='inner'
        )
        
        # 4. DEDUPLICATION LOGIC (The Fix)
        # We only want the mapping columns
        mapping_df = merged_df[['company_name', 'employer_name']]
        
        # If "Leidos" maps to both "LEIDOS INC" and "LEIDOS LLC", keep only the first one.
        # This ensures your final table doesn't explode with duplicates.
        mapping_df = mapping_df.drop_duplicates(subset=['company_name'], keep='first')
        
        print(f"Found {len(mapping_df)} unique matches.")

        # 5. Upload to BigQuery
        if not mapping_df.empty:
            client = hook.get_client()
            # write_disposition='WRITE_TRUNCATE' ensures we overwrite the temp table every day
            job_config = client.load_table_from_dataframe(
                mapping_df, 
                TABLE_MAPPING
            )
            job_config.result()
            print("Mapping table uploaded.")
        else:
            print("No matches found.")

    # Task: Create 'sponsor_companies' connecting ID, Name, and Sponsorship
    create_final_table = BigQueryInsertJobOperator(
        task_id="create_sponsor_companies",
        configuration={
            "query": {
                "query": f"""
                    CREATE OR REPLACE TABLE `{TABLE_FINAL_MATCH}` AS
                    SELECT
                        -- CONNECTING THE TABLES PROPERLY HERE
                        c.company_id,
                        c.company_name,
                        
                        -- Bring in the H1B data via the mapping
                        m.employer_name as h1b_official_name,
                        COALESCE(h.ever_sponsored_h1b, FALSE) as is_sponsor,
                        h.last_updated_fiscal_year
                        
                    FROM `{TABLE_COMPANIES}` c
                    
                    -- 1. Join Mapping (This is now 1-to-1, so no duplicates generated)
                    LEFT JOIN `{TABLE_MAPPING}` m
                        ON c.company_name = m.company_name
                        
                    -- 2. Join H1B Details
                    LEFT JOIN `{TABLE_H1B}` h
                        ON m.employer_name = h.employer_name
                """,
                "useLegacySql": False
            }
        },
        gcp_conn_id=GCP_CONN_ID,
    )

    build_mapping_table() >> create_final_table

company_matching_pipeline()