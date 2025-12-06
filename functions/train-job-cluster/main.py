import functions_framework
import json
import logging
from google.cloud import bigquery

# -------------------------------------------------------
# Cloud Function: train-job-cluster
# Trigger: HTTP
# -------------------------------------------------------
# This function performs your clustering job and writes
# the cluster assignments to BigQuery.
# -------------------------------------------------------

@functions_framework.http
def train_job_cluster(request):
    """
    HTTP Cloud Function that trains job clusters and writes results to BigQuery.
    """

    logging.info("Received request for train_job_cluster")

    try:
        # -------------------------------------------------------
        # Optional: parse data from request
        # -------------------------------------------------------
        if request.is_json:
            req_data = request.get_json(silent=True)
            logging.info(f"Request JSON: {req_data}")
        else:
            req_data = {}
            logging.info("No JSON body received.")

        # -------------------------------------------------------
        # Connect to BigQuery using Cloud Function's service account
        # -------------------------------------------------------
        bq_client = bigquery.Client()

        # Example: Run query to generate cluster assignments
        # Replace with your actual clustering logic
        query = """
        SELECT
            job_id,
            MOD(ABS(FARM_FINGERPRINT(job_id)), 10) AS cluster_id
        FROM `ba882-team4-474802.ba882_jobs.job_features`
        """

        logging.info("Running clustering query...")
        query_job = bq_client.query(query)
        results = query_job.result()

        # -------------------------------------------------------
        # Convert results to rows for insertion
        # -------------------------------------------------------
        rows_to_insert = [
            {"job_id": row.job_id, "cluster_id": row.cluster_id}
            for row in results
        ]

        logging.info(f"Prepared {len(rows_to_insert)} cluster assignments.")

        # -------------------------------------------------------
        # Insert into BigQuery table
        # -------------------------------------------------------
        table_id = "ba882-team4-474802.ba882_jobs.job_clusters"

        logging.info(f"Inserting into {table_id}...")
        errors = bq_client.insert_rows_json(table_id, rows_to_insert)

        if errors:
            logging.error(f"BigQuery insertion errors: {errors}")
            return (json.dumps({"status": "error", "errors": errors}), 500)

        logging.info("Successfully inserted cluster assignments into BigQuery.")

        # -------------------------------------------------------
        # Return success response
        # -------------------------------------------------------
        return json.dumps({
            "status": "success",
            "inserted_rows": len(rows_to_insert)
        }), 200

    except Exception as e:
        logging.exception("Error occurred during clustering job")
        return json.dumps({"status": "error", "message": str(e)}), 500
