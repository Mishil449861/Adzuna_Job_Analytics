# FILE: dags/skill_extraction_dag.py
#
# This is the corrected, final DAG.
# It now authenticates using your service account JSON file
# instead of the default Airflow connection.

import pendulum
import spacy
import os  # <-- ADDED THIS IMPORT
from spacy.pipeline import EntityRuler
from google.cloud import bigquery
from google.oauth2 import service_account  # <-- ADDED THIS IMPORT

# We no longer need the BigQueryHook
# from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.decorators import dag, task


# --- 1. DEFINE YOUR SKILL LIST ---
# (Comprehensive skill list from before)
SKILL_LIST = [
    # --- Core Programming & Scripting ---
    "Python", "SQL", "R", "Java", "C++", "C#", "Scala", "Go", "Bash", "Perl",
    # --- Data Science & ML Libraries ---
    "TensorFlow", "PyTorch", "Keras", "Scikit-learn", "Pandas", "NumPy",
    "SciPy", "Matplotlib", "Seaborn", "Plotly", "Statsmodels", "NLTK",
    "spaCy", "Hugging Face", "Transformers", "OpenCV", "LightGBM", "XGBoost",
    "Caret", "Tidyverse", "ggplot2", "Dplyr",
    # --- BI & Visualization Tools ---
    "Tableau", "Power BI", "Microsoft Power BI", "Looker", "Qlik", "QlikView",
    "Qlik Sense", "MicroStrategy", "SAS", "SSRS", "Domo", "Alteryx",
    "Google Data Studio", "Looker Studio", "Excel",
    # --- Big Data & Data Engineering ---
    "Spark", "Apache Spark", "PySpark", "Hadoop", "Kafka", "Apache Kafka",
    "Flink", "Apache Flink", "Airflow", "Apache Airflow", "Luigi", "Prefect",
    "Dagster", "Databricks", "Snowflake", "dbt", "Hive", "Apache Hive", "Pig",
    "Sqoop", "MapReduce", "Storm",
    # --- Cloud Platforms & Services ---
    "AWS", "GCP", "Google Cloud Platform", "Azure", "Microsoft Azure",
    "Amazon Web Services", "Redshift", "BigQuery", "S3", "EC2",
    "Azure Synapse Analytics", "Azure Data Factory", "AWS Glue", "EMR",
    "Amazon Kinesis", "Google Cloud Storage", "Google Pub/Sub",
    # --- Databases ---
    "PostgreSQL", "MySQL", "Microsoft SQL Server", "T-SQL", "PL/SQL",
    "Oracle", "MongoDB", "Cassandra", "Redis", "DynamoDB", "Elasticsearch",
    "Teradata", "NoSQL", "SQL Server",
    # --- MLOps & DevOps ---
    "Docker", "Kubernetes", "Git", "GitHub", "GitLab", "Jenkins", "Ansible",
    "Terraform", "CI/CD", "MLflow", "Kubeflow", "Seldon", "Dataiku",
    "Amazon SageMaker", "Azure Machine Learning", "Vertex AI",
    # --- Core Concepts & Fields ---
    "Machine Learning", "AI", "Artificial Intelligence", "Data Science",
    "Data Analytics", "Deep Learning", "NLP", "Natural Language Processing",
    "Computer Vision", "Data Engineering", "Business Intelligence"
]


# --- 2. CONFIGURATION (CORRECTED) ---
GCP_PROJECT_ID = "ba882-team4-474802"
GCP_DATASET_ID = "ba882_jobs"

SOURCE_TABLE_ID = f"{GCP_PROJECT_ID}.{GCP_DATASET_ID}.jobs"
DEST_TABLE_ID = f"{GCP_PROJECT_ID}.{GCP_DATASET_ID}.job_skills"
MODEL_VERSION = "entity_ruler_v1.1_comprehensive"

# Column names in your 'jobs' table
SOURCE_ID_COL = "job_id"
SOURCE_TEXT_COL = "description"


@dag(
    dag_id="skill_extraction_pipeline",
    schedule="0 0 */3 * *",  # <-- THIS IS THE FIX (was schedule_interval)
    start_date=pendulum.datetime(2025, 11, 1, tz="UTC"),
    catchup=False,
    tags=["skills", "spacy", "bigquery", "nlp"],
)
def skill_extraction_dag():
    """
    DAG to extract skills from job descriptions using spaCy and
    load them into a separate BigQuery table.
    This version authenticates using a service account JSON file.
    """

    def load_spacy_model_with_ruler():
        """Loads a blank spaCy model and adds the EntityRuler."""
        print("Loading spaCy model...")
        nlp = spacy.blank("en")
        ruler = nlp.add_pipe("entity_ruler")
        patterns = []
        for skill in SKILL_LIST:
            patterns.append({
                "label": "SKILL",
                "pattern": [{"LOWER": word.lower()} for word in skill.split()]
            })
        ruler.add_patterns(patterns)
        print(f"spaCy model with {len(patterns)} skill patterns loaded.")
        return nlp

    @task
    def extract_and_load_skills():
        """
        Reads new job descriptions from BigQuery, extracts skills,
        and loads them into the job_skills table.
        """
        nlp = load_spacy_model_with_ruler()

        # --- THIS IS THE MODIFIED AUTHENTICATION BLOCK ---
        print(f"Connecting to BigQuery using service account...")

        # Build a robust path to the include folder, relative to this DAG file
        # This is safer than a hardcoded path.
        dag_dir = os.path.dirname(os.path.abspath(__file__))
        key_path = os.path.join(dag_dir, "../include/ba882-team4-474802-bee53a65f2ac.json")

        # Create credentials from the file
        credentials = service_account.Credentials.from_service_account_file(key_path)

        # Create the BigQuery client, passing in the credentials
        # This is the correct client to use for BigQuery operations.
        client = bigquery.Client(credentials=credentials, project=GCP_PROJECT_ID)

        print("Successfully created BigQuery client.")
        # --- END OF MODIFIED BLOCK ---

        # 1. Read data from source table
        read_query = f"""
            SELECT
                source.{SOURCE_ID_COL},
                source.{SOURCE_TEXT_COL}
            FROM `{SOURCE_TABLE_ID}` AS source
            LEFT JOIN `{DEST_TABLE_ID}` AS skills
                ON source.{SOURCE_ID_COL} = skills.source_job_id
            WHERE source.{SOURCE_TEXT_COL} IS NOT NULL
                AND skills.source_job_id IS NULL
        """

        print(f"Executing query to find new jobs: {read_query}")
        query_job = client.query(read_query)

        rows_to_insert = []
        extraction_time = pendulum.now("UTC")

        print("Beginning skill extraction on new jobs...")
        job_count = 0
        skill_count = 0

        # 2. Process text with spaCy
        for row in query_job:
            job_count += 1
            job_id = row[SOURCE_ID_COL]
            text = row[SOURCE_TEXT_COL]
            doc = nlp(text)
            found_skills_for_job = set()
            for ent in doc.ents:
                if ent.label_ == "SKILL":
                    found_skills_for_job.add(ent.text.lower())

            # 3. Prepare data for insertion
            for skill_name in found_skills_for_job:
                skill_count += 1
                rows_to_insert.append({
                    "source_job_id": job_id,
                    "skill_name": skill_name,
                    "extraction_date": extraction_time.to_iso8601_string(),
                    "model_version": MODEL_VERSION
                })

            # 4. Insert in batches
            if len(rows_to_insert) >= 500:
                print(f"Inserting batch of {len(rows_to_insert)} skills...")
                errors = client.insert_rows_json(DEST_TABLE_ID, rows_to_insert)
                if errors:
                    print(f"Errors encountered during BQ insert: {errors}")
                rows_to_insert = []

        # 5. Insert any remaining rows
        if rows_to_insert:
            print(f"Inserting final batch of {len(rows_to_insert)} skills...")
            errors = client.insert_rows_json(DEST_TABLE_ID, rows_to_insert)
            if errors:
                print(f"Errors encountered during BQ insert: {errors}")

        if job_count == 0:
            print("Extraction complete. No new jobs to process.")
        else:
            print(f"Extraction complete. Processed {job_count} new jobs and found {skill_count} skill entries.")

    # --- DEFINE THE TASK FLOW ---
    extract_and_load_skills()

# This instantiates the DAG
skill_extraction_dag()