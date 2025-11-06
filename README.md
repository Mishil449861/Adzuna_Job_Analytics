Adzuna Data Pipeline

This project is an automated data pipeline built with Apache Airflow and deployed on Astronomer. It fetches job postings from the Adzuna API, transforms the data into a star schema, and loads it into Google BigQuery.

Tech Stack

Orchestration: Apache Airflow

Deployment: Astronomer (Astro CLI)

Cloud Provider: Google Cloud Platform (GCP)

Data Warehouse: Google BigQuery

Storage: Google Cloud Storage (GCS) for staging

Secrets: Google Secret Manager

Pipeline Overview

The pipeline (adzuna_gcs_to_bigquery_star_schema_v2) runs daily and performs the following steps:

Get Secrets: Fetches Adzuna API keys from Google Secret Manager.

Extract & Transform: Calls the Adzuna API, fetches 250 jobs, and transforms the JSON data into 5 Pandas DataFrames that follow a star schema.

Load to GCS: Saves these 5 DataFrames as Parquet files in a GCS bucket (adzuna-bucket).

Load to Staging: Loads the Parquet files from GCS into 5 temporary "staging" tables in BigQuery.

Merge to Production: Runs 5 SQL MERGE statements to idempotently upsert the data from the staging tables into the final production tables (jobs, companies, locations, categories, jobstats).

New jobs are INSERTED.

Existing jobs (by job_id) are UPDATED.

This ensures data is never duplicated, even if the pipeline runs multiple times a day.

Data Schema (ba882_jobs dataset)

Fact Tables: jobs, jobstats

Dimension Tables: companies, locations, categories