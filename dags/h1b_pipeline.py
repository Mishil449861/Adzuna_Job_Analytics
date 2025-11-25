import pendulum
from airflow.decorators import dag, task
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

# --- CONFIGURATION ---
GCP_PROJECT_ID = "ba882-team4-474802"
GCP_DATASET_ID = "ba882_jobs"
GCP_CONN_ID = "gcp_default"

@dag(
    dag_id="h1b_static_data_pipeline",
    schedule=None,  # Manual Trigger Only (since data is static)
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["h1b", "static", "bigquery"],
)
def h1b_pipeline():

    # Task 1: Transform Raw Staging Data to Analytical Table
    # This aggregates the 12 approval/denial columns into 4 useful metrics
    transform_h1b_data = BigQueryInsertJobOperator(
        task_id="transform_h1b_data",
        configuration={
            "query": {
                "query": f"""
                    INSERT INTO `{GCP_PROJECT_ID}.{GCP_DATASET_ID}.h1b_petitions` 
                    (fiscal_year, employer_name, naics_code, city, state, zip_code, 
                     total_initial_approvals, total_initial_denials, 
                     total_continuing_approvals, total_continuing_denials)
                    SELECT
                        fiscal_year,
                        employer_name,
                        naics_code,
                        city,
                        state,
                        zip_code,
                        -- Aggregating Initial Approvals
                        (IFNULL(new_employment_approval, 0) + IFNULL(new_concurrent_approval, 0) + IFNULL(change_employer_approval, 0)),
                        -- Aggregating Initial Denials
                        (IFNULL(new_employment_denial, 0) + IFNULL(new_concurrent_denial, 0) + IFNULL(change_employer_denial, 0)),
                        -- Aggregating Continuing Approvals
                        (IFNULL(continuation_approval, 0) + IFNULL(change_same_employer_approval, 0) + IFNULL(amended_approval, 0)),
                        -- Aggregating Continuing Denials
                        (IFNULL(continuation_denial, 0) + IFNULL(change_same_employer_denial, 0) + IFNULL(amended_denial, 0))
                    FROM `{GCP_PROJECT_ID}.{GCP_DATASET_ID}.h-1b_sponsorship`
                """,
                "useLegacySql": False
            }
        },
        gcp_conn_id=GCP_CONN_ID,
    )

    # Task 2: Update Company Sponsorship Status (Side-by-Side View)
    # Uses MERGE to keep a permanent list of companies that have EVER sponsored
    update_sponsorship_status = BigQueryInsertJobOperator(
        task_id="update_sponsorship_status",
        configuration={
            "query": {
                "query": f"""
                    MERGE INTO `{GCP_PROJECT_ID}.{GCP_DATASET_ID}.company_sponsorship_status` AS target
                    USING (
                        SELECT 
                            employer_name,
                            MAX(fiscal_year) as fiscal_year,
                            -- If they have ANY approvals in this batch, they are a sponsor
                            MAX(CASE WHEN (
                                new_employment_approval + continuation_approval + 
                                change_same_employer_approval + new_concurrent_approval + 
                                change_employer_approval + amended_approval
                            ) > 0 THEN TRUE ELSE FALSE END) as is_sponsor
                        FROM `{GCP_PROJECT_ID}.{GCP_DATASET_ID}.h-1b_sponsorship`
                        GROUP BY employer_name
                    ) AS source
                    ON target.employer_name = source.employer_name
                    
                    -- If company exists, update status only if they become a sponsor (never revert to False)
                    WHEN MATCHED THEN
                      UPDATE SET 
                        target.ever_sponsored_h1b = CASE 
                            WHEN target.ever_sponsored_h1b = TRUE THEN TRUE 
                            WHEN source.is_sponsor = TRUE THEN TRUE 
                            ELSE target.ever_sponsored_h1b 
                        END,
                        target.last_updated_fiscal_year = GREATEST(target.last_updated_fiscal_year, source.fiscal_year)
                    
                    -- If new company, insert them
                    WHEN NOT MATCHED THEN
                      INSERT (employer_name, ever_sponsored_h1b, last_updated_fiscal_year)
                      VALUES (source.employer_name, source.is_sponsor, source.fiscal_year);
                """,
                "useLegacySql": False
            }
        },
        gcp_conn_id=GCP_CONN_ID,
    )

    # Pipeline Flow
    transform_h1b_data >> update_sponsorship_status

h1b_pipeline()