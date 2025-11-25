# FILE: dags/skill_extraction_dag.py
#
# This DAG extracts skills from job descriptions using spaCy and regex.
# It also maintains the star schema by updating dimension and junction tables.
#
# Features:
# - Runs Daily
# - Extracts Hard Skills (via spaCy EntityRuler)
# - Extracts Soft Skills (via expanded list)
# - Extracts Years of Experience (via Regex)
# - Updates 'skills' dimension table
# - Updates 'job_skills_bridge' junction table
# - Updates 'company_presence' junction table
# - Updates 'category_standards' junction table
# - Updates 'category_industry_overlap' junction table

import pendulum
import spacy
import os
import re
from spacy.pipeline import EntityRuler
from google.cloud import bigquery
from google.oauth2 import service_account
from airflow.decorators import dag, task
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

# --- 1. EXPANDED SKILL LIST (Tech + Soft Skills + Concepts) ---
SKILL_LIST = [
    # --- Core Tech & Languages ---
    "Python", "SQL", "R", "Java", "C++", "C#", "Scala", "Go", "Bash", "Perl",
    "JavaScript", "TypeScript", "HTML", "CSS", "React", "Angular", "Vue",
    "Swift", "Kotlin", "Ruby", "PHP", "Matlab", "Groovy", "Julia",
    
    # --- Data Science & ML Libraries ---
    "TensorFlow", "PyTorch", "Keras", "Scikit-learn", "Pandas", "NumPy", "SciPy",
    "Statsmodels", "NLTK", "spaCy", "Hugging Face", "Transformers", "OpenCV", 
    "LightGBM", "XGBoost", "Caret", "Tidyverse", "ggplot2", "Dplyr", "Seaborn",
    "Matplotlib", "Plotly", "Altair", "Bokeh",
    
    # --- Big Data & Data Engineering ---
    "Spark", "Apache Spark", "PySpark", "Hadoop", "Kafka", "Apache Kafka",
    "Airflow", "Apache Airflow", "dbt", "Snowflake", "Databricks", "BigQuery",
    "Redshift", "Synapse", "Fabric", "Presto", "Trino", "Hive", "Pig", "Flink",
    "Storm", "Beam", "Dataflow", "Glue", "Athena", "Kinesis", "Pub/Sub",
    
    # --- BI & Visualization ---
    "Tableau", "Power BI", "Looker", "Qlik", "QlikView", "Qlik Sense", 
    "Excel", "Google Sheets", "SAS", "SPSS", "MicroStrategy", "Cognos", 
    "Domo", "Alteryx", "Knime", "Spotfire",
    
    # --- Cloud, DevOps & Infrastructure ---
    "AWS", "GCP", "Google Cloud Platform", "Azure", "Docker", "Kubernetes", 
    "Terraform", "Jenkins", "Git", "GitHub", "GitLab", "Bitbucket", "CI/CD", 
    "Linux", "Unix", "Ansible", "Puppet", "Chef", "Vagrant", "OpenStack",
    
    # --- Databases (SQL & NoSQL) ---
    "PostgreSQL", "MySQL", "Microsoft SQL Server", "T-SQL", "PL/SQL", "Oracle", 
    "MongoDB", "Cassandra", "Redis", "DynamoDB", "Elasticsearch", "Teradata", 
    "NoSQL", "MariaDB", "SQLite", "Neo4j",
    
    # --- Soft Skills (70+ Valued in Data Teams) ---
    "Communication", "Written Communication", "Verbal Communication", "Leadership", 
    "Problem Solving", "Critical Thinking", "Teamwork", "Collaboration", 
    "Agile", "Scrum", "Kanban", "Project Management", "Time Management", 
    "Adaptability", "Flexibility", "Creativity", "Mentoring", "Coaching", 
    "Presentation Skills", "Storytelling", "Data Storytelling", "Negotiation", 
    "Conflict Resolution", "Emotional Intelligence", "Empathy", "Active Listening", 
    "Technical Writing", "Documentation", "Organization", "Prioritization", 
    "Decision Making", "Strategic Thinking", "Business Acumen", "Stakeholder Management", 
    "Client Facing", "Consulting", "Analytical Skills", "Attention to Detail", 
    "Curiosity", "Continuous Learning", "Self-Motivation", "Resilience", 
    "Interpersonal Skills", "Public Speaking", "Networking", "Brainstorming", 
    "Innovation", "Design Thinking", "Customer Service", "Troubleshooting", 
    "Debugging", "Research", "Planning", "Scheduling", "Budgeting", 
    "Resource Management", "Risk Management", "Change Management", "Work Ethic",
    "Dependability", "Integrity", "Initiative", "Influence", "Persuasion",
    
    # --- Concepts & Methodologies ---
    "Data Analysis", "Machine Learning", "Deep Learning", "NLP", "Natural Language Processing",
    "Statistics", "Mathematics", "Computer Vision", "Generative AI", "LLM", 
    "Large Language Models", "RAG", "Retrieval-Augmented Generation",
    "Data Engineering", "Data Science", "Business Intelligence", "ETL", "ELT", 
    "Data Warehousing", "Data Modeling", "Data Quality", "Data Governance",
    "A/B Testing", "Experimentation", "Predictive Modeling", "Classification",
    "Regression", "Clustering", "Optimization"
]

# --- 2. CONFIGURATION ---
GCP_PROJECT_ID = "ba882-team4-474802"
GCP_DATASET_ID = "ba882_jobs"
GCP_CONN_ID = "gcp_default"

SOURCE_TABLE_ID = f"{GCP_PROJECT_ID}.{GCP_DATASET_ID}.jobs"
DEST_TABLE_ID = f"{GCP_PROJECT_ID}.{GCP_DATASET_ID}.job_skills"
MODEL_VERSION = "entity_ruler_v2.0_comprehensive"

SOURCE_ID_COL = "job_id"
SOURCE_TEXT_COL = "description"
NEW_KEY_FILENAME = "ba882-team4-474802-bee53a65f2ac.json"

# --- SQL Paths ---
SQL_DIR = "/usr/local/airflow/include/sql"

@dag(
    dag_id="skill_extraction_pipeline",
    schedule="0 0 * * *",  # Runs DAILY at midnight
    start_date=pendulum.datetime(2025, 11, 1, tz="UTC"),
    catchup=False,
    tags=["skills", "spacy", "bigquery", "nlp", "etl"],
)
def skill_extraction_dag():

    def load_spacy_model_with_ruler():
        """Loads spaCy with EntityRuler for skills."""
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
        return nlp

    def extract_experience(text):
        """Extracts years of experience using Regex."""
        # Matches "5 years", "5+ years", "5-7 years", "5 yrs"
        pattern = r"(\d+(\+)?-?\d*)\s+(years?|yrs?)"
        matches = re.findall(pattern, text.lower())
        if matches:
            # Return the first match string (e.g., "5+") to verify existence
            return matches[0][0]
        return None

    @task
    def extract_and_load_skills():
        """
        Reads new job descriptions, extracts skills/experience,
        and loads them into the job_skills table.
        """
        nlp = load_spacy_model_with_ruler()
        print(f"Connecting to BigQuery...")
        
        # Key-file authentication for this task
        key_path = f"/usr/local/airflow/include/{NEW_KEY_FILENAME}"
        credentials = service_account.Credentials.from_service_account_file(key_path)
        client = bigquery.Client(credentials=credentials, project=GCP_PROJECT_ID)

        # 1. Read NEW jobs (Idempotent check)
        # Only process jobs that don't have skills yet
        read_query = f"""
            SELECT source.{SOURCE_ID_COL}, source.{SOURCE_TEXT_COL}
            FROM `{SOURCE_TABLE_ID}` AS source
            LEFT JOIN `{DEST_TABLE_ID}` AS skills
                ON source.{SOURCE_ID_COL} = skills.source_job_id
            WHERE source.{SOURCE_TEXT_COL} IS NOT NULL
                AND skills.source_job_id IS NULL
        """
        query_job = client.query(read_query)
        
        rows_to_insert = []
        extraction_time = pendulum.now("UTC")
        
        # 2. Process text
        print("Processing jobs...")
        for row in query_job:
            job_id = row[SOURCE_ID_COL]
            text = row[SOURCE_TEXT_COL]
            doc = nlp(text)
            found_items = set()
            
            # A. Find Skills (spaCy)
            for ent in doc.ents:
                if ent.label_ == "SKILL":
                    found_items.add(ent.text.lower())
            
            # B. Find Experience (Regex) - Add as a "skill" for now
            exp = extract_experience(text)
            if exp:
                found_items.add(f"{exp} years experience")

            # 3. Prepare rows
            for item in found_items:
                rows_to_insert.append({
                    "source_job_id": job_id,
                    "skill_name": item,
                    "extraction_date": extraction_time.to_iso8601_string(),
                    "model_version": MODEL_VERSION
                })

            # 4. Batch Insert
            if len(rows_to_insert) >= 1000:
                client.insert_rows_json(DEST_TABLE_ID, rows_to_insert)
                rows_to_insert = []

        if rows_to_insert:
            client.insert_rows_json(DEST_TABLE_ID, rows_to_insert)
            print(f"Inserted {len(rows_to_insert)} remaining rows.")

    # --- SQL TASKS for Junction Tables ---
    # These tasks run AFTER the python extraction is complete.
    # They maintain the "normalized" tables and junction/bridge tables.
    
    # 1. Update Skills Dimension (update_skills_dimension.sql)
    # Adds any newly found skills to the 'skills' table
    update_skills_dim = BigQueryInsertJobOperator(
        task_id="update_skills_dim",
        configuration={
            "query": {
                "query": "{% include 'sql/update_skills_dimension.sql' %}", 
                "useLegacySql": False
            }
        },
        gcp_conn_id=GCP_CONN_ID,
    )

    # 2. Update Job-Skills Bridge (update_job_skills_bridge.sql)
    # Links jobs to skill IDs in 'job_skills_bridge'
    update_skills_bridge = BigQueryInsertJobOperator(
        task_id="update_skills_bridge",
        configuration={
            "query": {
                "query": "{% include 'sql/update_job_skills_bridge.sql' %}", 
                "useLegacySql": False
            }
        },
        gcp_conn_id=GCP_CONN_ID,
    )
    
    # 3. Update Company Presence (update_company_presence.sql)
    # Refreshes the Company-Location link
    update_company_presence = BigQueryInsertJobOperator(
        task_id="update_company_presence",
        configuration={
            "query": {
                "query": "{% include 'sql/update_company_presence.sql' %}", 
                "useLegacySql": False
            }
        },
        gcp_conn_id=GCP_CONN_ID,
    )

    # 4. Update Category Standards (update_category_standards.sql)
    # Recalculates the standard skills for each category
    update_category_standards = BigQueryInsertJobOperator(
        task_id="update_category_standards",
        configuration={
            "query": {
                "query": "{% include 'sql/update_category_standards.sql' %}", 
                "useLegacySql": False
            }
        },
        gcp_conn_id=GCP_CONN_ID,
    )
    
    # 5. Update Category-Industry Overlap (update_category_industry_overlap.sql)
    # Recalculates top companies per category
    update_category_industry = BigQueryInsertJobOperator(
        task_id="update_category_industry",
        configuration={
            "query": {
                "query": "{% include 'sql/update_category_industry_overlap.sql' %}", 
                "useLegacySql": False
            }
        },
        gcp_conn_id=GCP_CONN_ID,
    )

    # --- Task Dependencies ---
    extraction_task = extract_and_load_skills()
    
    # Run extraction first, then update all downstream tables in parallel
    extraction_task >> [
        update_skills_dim,
        update_skills_bridge, 
        update_company_presence, 
        update_category_standards,
        update_category_industry
    ]

skill_extraction_dag()