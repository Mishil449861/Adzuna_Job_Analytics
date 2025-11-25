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
TABLE_FINAL_MATCH = f"{GCP_PROJECT_ID}.{GCP_DATASET_ID}.company_h1b_match"

def normalize_company_name(name):
    """
    Python logic to standardize company names.
    Turns 'Google LLC.' -> 'google'
    Turns 'Amazon.com Inc' -> 'amazon'
    """
    if not name:
        return ""
    
    # 1. Lowercase and strip whitespace
    clean = str(name).lower().strip()
    
    # 2. Remove text inside parentheses (e.g., "Google (Alphabet)")
    clean = re.sub(r'\([^)]*\)', '', clean)
    
    # 3. Remove common legal suffixes
    # Note: 'cleanco' library does this even better, but this regex works without extra installs
    suffixes = [
        r'\binc\.?\b', r'\bllc\.?\b', r'\bltd\.?\b', r'\bcorp\.?\b', 
        r'\bcorporation\b', r'\bco\.?\b', r'\bcompany\b', r'\bpllc\b', 
        r'\bpbc\b', r'\bgroup\b', r'\bholdings\b', r'\btechnologies\b'
    ]
    pattern = '|'.join(suffixes)
    clean = re.sub(pattern, '', clean)
    
    # 4. Remove punctuation (keep numbers) and extra spaces
    clean = re.sub(r'[^\w\s]', '', clean)
    clean = re.sub(r'\s+', ' ', clean).strip()
    
    return clean

@dag(
    dag_id="company_matching_pipeline",
    schedule="@daily",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["h1b", "matching", "bigquery", "python"],
)
def company_matching_pipeline():

    @task
    def build_mapping_table():
        """
        Downloads distinct names, normalizes them, finds matches, 
        and uploads a mapping table to BigQuery.
        """
        hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID)
        
        # 1. Get distinct Adzuna Names
        df_adzuna = hook.get_pandas_df(f"SELECT DISTINCT company_name FROM `{TABLE_COMPANIES}` WHERE company_name IS NOT NULL")
        
        # 2. Get distinct H1B Names
        df_h1b = hook.get_pandas_df(f"SELECT DISTINCT employer_name FROM `{TABLE_H1B}` WHERE employer_name IS NOT NULL")
        
        print(f"Fetched {len(df_adzuna)} Adzuna names and {len(df_h1b)} H1B names.")

        # 3. Normalize both sides
        df_adzuna['clean_name'] = df_adzuna['company_name'].apply(normalize_company_name)
        df_h1b['clean_name'] = df_h1b['employer_name'].apply(normalize_company_name)

        # 4. Perform the Match (Inner Join on the normalized name)
        # This connects 'Google' (Adzuna) to 'Google LLC' (H1B) because both clean to 'google'
        merged_df = pd.merge(
            df_adzuna, 
            df_h1b, 
            on='clean_name', 
            how='inner'
        )
        
        # Select only the map: Original Adzuna Name -> Original H1B Name
        mapping_df = merged_df[['company_name', 'employer_name']].drop_duplicates()
        
        print(f"Found {len(mapping_df)} matches using normalization.")

        # 5. Upload this mapping back to BigQuery so SQL can use it
        # Using 'replace' to overwrite previous day's mapping
        if not mapping_df.empty:
            # Need to use a client to upload dataframe
            client = hook.get_client()
            job_config = client.load_table_from_dataframe(mapping_df, TABLE_MAPPING)
            job_config.result() # Wait for completion
            print("Mapping table uploaded successfully.")
        else:
            print("No matches found. Table not updated.")

    # Task: Use the mapping table to create the final rich dataset
    create_final_table = BigQueryInsertJobOperator(
        task_id="match_companies",
        configuration={
            "query": {
                "query": f"""
                    CREATE OR REPLACE TABLE `{TABLE_FINAL_MATCH}` AS
                    SELECT
                        c.company_id,
                        c.company_name as adzuna_name,
                        -- Use the mapped H1B name if it exists
                        m.employer_name as h1b_matched_name,
                        
                        -- Join actual H1B stats using the mapped name
                        COALESCE(h.ever_sponsored_h1b, FALSE) as is_sponsor,
                        h.last_updated_fiscal_year,
                        
                        -- Flag to show how we found them
                        CASE 
                            WHEN m.employer_name IS NOT NULL THEN 'Normalized Match'
                            ELSE 'No Match'
                        END as match_method

                    FROM `{TABLE_COMPANIES}` c
                    
                    -- 1. Join to our Python-generated mapping table
                    LEFT JOIN `{TABLE_MAPPING}` m
                        ON c.company_name = m.company_name
                    
                    -- 2. Join to the H1B table using the MAPPED name (which links 'Google' to 'Google LLC')
                    LEFT JOIN `{TABLE_H1B}` h
                        ON m.employer_name = h.employer_name
                """,
                "useLegacySql": False
            }
        },
        gcp_conn_id=GCP_CONN_ID,
    )

    # Dependency: Build map first, then run SQL
    build_mapping_table() >> create_final_table

company_matching_pipeline()