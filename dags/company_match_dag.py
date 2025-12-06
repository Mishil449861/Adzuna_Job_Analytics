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
TABLE_FINAL_MATCH = f"{GCP_PROJECT_ID}.{GCP_DATASET_ID}.sponsor_companies"

def normalize_company_name(name):
    """
    Standardizes company names to maximize matches.
    'System One Holdings LLC' -> 'system one'
    """
    if not name:
        return ""
    
    # 1. Lowercase and strip whitespace
    clean = str(name).lower().strip()
    
    # 2. Remove text inside parentheses (e.g., "Google (Alphabet)")
    clean = re.sub(r'\([^)]*\)', '', clean)
    
    # 3. Remove common corporate suffixes
    suffixes = [
        r'\binc\.?\b', r'\bllc\.?\b', r'\bltd\.?\b', r'\bcorp\.?\b', 
        r'\bcorporation\b', r'\bco\.?\b', r'\bcompany\b', r'\bpllc\b', 
        r'\bpbc\b', r'\bgroup\b', r'\bholdings\b', r'\btechnologies\b',
        r'\bsolutions\b', r'\bservices\b', r'\bpartners\b', r'\binternational\b'
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
        Creates a clean 1-to-1 mapping table between Adzuna Names and H1B Names.
        """
        hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID)
        
        # 1. Get distinct Names (Using Standard SQL to avoid 'Invalid Project ID' error)
        df_adzuna = hook.get_pandas_df(
            sql=f"SELECT DISTINCT company_name FROM `{TABLE_COMPANIES}` WHERE company_name IS NOT NULL",
            dialect='standard'
        )
        
        df_h1b = hook.get_pandas_df(
            sql=f"SELECT DISTINCT employer_name FROM `{TABLE_H1B}` WHERE employer_name IS NOT NULL",
            dialect='standard'
        )
        
        print(f"Fetched {len(df_adzuna)} Adzuna names and {len(df_h1b)} H1B names.")

        # 2. Normalize Names
        df_adzuna['clean_name'] = df_adzuna['company_name'].apply(normalize_company_name)
        df_h1b['clean_name'] = df_h1b['employer_name'].apply(normalize_company_name)

        # 3. Merge on the clean name
        merged_df = pd.merge(
            df_adzuna, 
            df_h1b, 
            on='clean_name', 
            how='inner'
        )
        
        # 4. DEDUPLICATION LOGIC
        # Only keep the relevant columns
        mapping_df = merged_df[['company_name', 'employer_name']]
        
        # If "Leidos" maps to both "LEIDOS INC" and "LEIDOS LLC", keep only the first match.
        # This prevents duplicates in the final join.
        mapping_df = mapping_df.drop_duplicates(subset=['company_name'], keep='first')
        
        print(f"Found {len(mapping_df)} unique matches.")

        # 5. Upload to BigQuery
        if not mapping_df.empty:
            client = hook.get_client()
            # load_table_from_dataframe automatically creates the table if missing
            job_config = client.load_table_from_dataframe(
                mapping_df, 
                TABLE_MAPPING
            )
            job_config.result() # Wait for completion
            print(f"Mapping table uploaded to {TABLE_MAPPING}.")
        else:
            print("No matches found. Mapping table not updated.")

    # Task: Create 'sponsor_companies' - ONLY KEEPING TRUE SPONSORS
    create_final_table = BigQueryInsertJobOperator(
        task_id="create_sponsor_companies",
        configuration={
            "query": {
                "query": f"""
                    CREATE OR REPLACE TABLE `{TABLE_FINAL_MATCH}` AS
                    SELECT
                        c.company_id,
                        c.company_name,
                        m.employer_name as h1b_official_name,
                        h.ever_sponsored_h1b as is_sponsor,
                        h.last_updated_fiscal_year
                        
                    FROM `{TABLE_COMPANIES}` c
                    
                    -- 1. INNER JOIN to Mapping: Drops anyone who didn't match a name
                    INNER JOIN `{TABLE_MAPPING}` m
                        ON c.company_name = m.company_name
                        
                    -- 2. INNER JOIN to H1B Data: Drops anyone missing from H1B table
                    INNER JOIN `{TABLE_H1B}` h
                        ON m.employer_name = h.employer_name
                    
                    -- 3. FILTER: STRICTLY Keep only True Sponsors
                    WHERE h.ever_sponsored_h1b = TRUE
                """,
                "useLegacySql": False
            }
        },
        gcp_conn_id=GCP_CONN_ID,
    )

    build_mapping_table() >> create_final_table

company_matching_pipeline()