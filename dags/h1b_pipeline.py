import pendulum
from airflow.decorators import dag, task
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

# --- CONFIGURATION ---
GCP_PROJECT_ID = "ba882-team4-474802"
GCP_DATASET_ID = "ba882_jobs"
GCP_CONN_ID = "gcp_default"

@dag(
    dag_id="h1b_static_data_pipeline",
    schedule=None,  # Manual Trigger Only
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["h1b", "static", "bigquery"],
)
def h1b_pipeline():

    # Task 1: Transform Raw Staging Data to Analytical Table
    # We use TRUNCATE first to ensure we don't get duplicates if we re-run this.
    transform_h1b_data = BigQueryInsertJobOperator(
        task_id="transform_h1b_data",
        configuration={
            "query": {
                "query": f"""
                    -- 1. Clear existing data to allow re-runs
                    TRUNCATE TABLE `{GCP_PROJECT_ID}.{GCP_DATASET_ID}.h1b_petitions`;

                    -- 2. Insert transformed data
                    INSERT INTO `{GCP_PROJECT_ID}.{GCP_DATASET_ID}.h1b_petitions` 
                    (fiscal_year, employer_name, naics_code, city, state, zip_code, 
                     total_initial_approvals, total_initial_denials, 
                     total_continuing_approvals, total_continuing_denials)
                    SELECT
                        fiscal_year,
                        employer_petitioner_name AS employer_name, -- Corrected Column Name
                        industry_naics_code AS naics_code,         -- Corrected Column Name
                        petitioner_city AS city,                   -- Corrected Column Name
                        petitioner_state AS state,                 -- Corrected Column Name
                        CAST(petitioner_zip_code AS STRING) AS zip_code, -- Fix Float to String mismatch
                        
                        -- Aggregating Initial Approvals
                        (IFNULL(new_employment_approval, 0) + IFNULL(new_concurrent_approval, 0) + IFNULL(change_of_employer_approval, 0)),
                        
                        -- Aggregating Initial Denials
                        (IFNULL(new_employment_denial, 0) + IFNULL(new_concurrent_denial, 0) + IFNULL(change_of_employer_denial, 0)),
                        
                        -- Aggregating Continuing Approvals
                        (IFNULL(continuation_approval, 0) + IFNULL(change_with_same_employer_approval, 0) + IFNULL(amended_approval, 0)),
                        
                        -- Aggregating Continuing Denials
                        (IFNULL(continuation_denial, 0) + IFNULL(change_with_same_employer_denial, 0) + IFNULL(amended_denial, 0))
                    FROM `{GCP_PROJECT_ID}.{GCP_DATASET_ID}.h-1b_sponsorship`
                """,
                "useLegacySql": False
            }
        },
        gcp_conn_id=GCP_CONN_ID,
    )

    # Task 2: Update Company Sponsorship Status (Merge)
    update_sponsorship_status = BigQueryInsertJobOperator(
        task_id="update_sponsorship_status",
        configuration={
            "query": {
                "query": f"""
                    MERGE INTO `{GCP_PROJECT_ID}.{GCP_DATASET_ID}.company_sponsorship_status` AS target
                    USING (
                        SELECT 
                            employer_petitioner_name AS employer_name, -- Corrected Column Name
                            MAX(fiscal_year) as fiscal_year,
                            -- If they have ANY approvals in this batch, they are a sponsor
                            MAX(CASE WHEN (
                                IFNULL(new_employment_approval, 0) + 
                                IFNULL(continuation_approval, 0) + 
                                IFNULL(change_with_same_employer_approval, 0) + 
                                IFNULL(new_concurrent_approval, 0) + 
                                IFNULL(change_of_employer_approval, 0) + 
                                IFNULL(amended_approval, 0)
                            ) > 0 THEN TRUE ELSE FALSE END) as is_sponsor
                        FROM `{GCP_PROJECT_ID}.{GCP_DATASET_ID}.h-1b_sponsorship`
                        GROUP BY employer_petitioner_name
                    ) AS source
                    ON target.employer_name = source.employer_name
                    
                    -- Update existing companies
                    WHEN MATCHED THEN
                      UPDATE SET 
                        target.ever_sponsored_h1b = CASE 
                            WHEN target.ever_sponsored_h1b = TRUE THEN TRUE 
                            WHEN source.is_sponsor = TRUE THEN TRUE 
                            ELSE target.ever_sponsored_h1b 
                        END,
                        target.last_updated_fiscal_year = GREATEST(target.last_updated_fiscal_year, source.fiscal_year)
                    
                    -- Insert new companies
                    WHEN NOT MATCHED THEN
                      INSERT (employer_name, ever_sponsored_h1b, last_updated_fiscal_year)
                      VALUES (source.employer_name, source.is_sponsor, source.fiscal_year);
                """,
                "useLegacySql": False
            }
        },
        gcp_conn_id=GCP_CONN_ID,
    )

    transform_h1b_data >> update_sponsorship_status

h1b_pipeline()