import streamlit as st
import pandas as pd
import altair as alt
from google.cloud import bigquery
from openai import OpenAI

# ============================================================
# SETUP
# ============================================================
st.set_page_config(page_title="📊 AI Cluster Dashboard", layout="wide")
st.title("📊 AI-Enhanced Job Cluster Dashboard")
st.write("Automatically generated cluster insights using K-Means + GPT")

client_bq = bigquery.Client()
client_ai = OpenAI(api_key=st.secrets["OPEN_AI_API"])

# ============================================================
# LOAD CLUSTER SUMMARY
# ============================================================
@st.cache_data
def load_cluster_summary():
    query = """
        SELECT
          c.cluster_id,
          COUNT(*) AS job_count,
          ARRAY_AGG(j.title ORDER BY j.title LIMIT 20) AS titles,
          AVG(j.salary_min) AS avg_salary_min,
          AVG(j.salary_max) AS avg_salary_max
        FROM `ba882-team4-474802.ba882_jobs.job_clusters` c
        JOIN `ba882-team4-474802.ba882_jobs.jobs` j
            ON c.job_id = j.job_id
        GROUP BY c.cluster_id
        ORDER BY c.cluster_id
    """
    return client_bq.query(query).to_dataframe()

cluster_df = load_cluster_summary()

if cluster_df.empty:
    st.error("⚠ No cluster data available.")
    st.stop()

# ============================================================
# AI-BASED CLUSTER NAMING — FIXED N/A ISSUE
# ============================================================
def generate_cluster_name(titles):
    prompt = f"""
    You are an expert labor market analyst.
    These job titles belong to a job cluster.
    Your task:
    - Generate a short cluster name (3–5 words)
    - Ignore or filter out job titles unrelated to data, AI, analytics, cloud, engineering.
    - Do NOT create names related to retail, healthcare, construction, music, or unrelated fields.

    Titles:
    {titles}

    Output ONLY the cluster name.
    """

    response = client_ai.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "You generate names for data-related job clusters."},
            {"role": "user", "content": prompt}
        ]
    )

    name = response.choices[0].message.content.strip()

    # ---- FIX: Prevent invalid cluster names ----
    if name.lower() in ["n/a", "na", "none", "", "null"]:
        name = "Unnamed Data Cluster"

    return name


cluster_df["cluster_name"] = cluster_df["titles"].apply(lambda t: generate_cluster_name(t))

# ============================================================
# SUMMARY TABLE (Hide cluster_id)
# ============================================================
st.subheader("📌 Cluster Summary")

summary_df = cluster_df[[
    "cluster_id",
    "cluster_name",
    "job_count",
    "avg_salary_min",
    "avg_salary_max"
]]

st.dataframe(
    summary_df.drop(columns=["cluster_id"]),  # hide ID
    use_container_width=True
)

# ============================================================
# VISUAL 1 — JOB COUNT BY CLUSTER
# ============================================================
st.markdown("### 📈 Job Count per Cluster")

chart_count = (
    alt.Chart(summary_df)
    .mark_bar(color="#4BA3C3")
    .encode(
        x=alt.X("cluster_id:O", title="Cluster ID", axis=alt.Axis(labelAngle=0)),
        y=alt.Y("job_count:Q", title="Number of Jobs"),
        tooltip=["cluster_id", "cluster_name", "job_count"]
    )
    .properties(height=350)
)

st.altair_chart(chart_count, use_container_width=True)

# ============================================================
# VISUAL 2 — SALARY COMPARISON
# ============================================================
st.markdown("### 💰 Average Salary by Cluster")

salary_chart = (
    alt.Chart(summary_df)
    .mark_bar(color="#8BC34A")
    .encode(
        x=alt.X("cluster_id:O", title="Cluster ID", axis=alt.Axis(labelAngle=0)),
        y=alt.Y("avg_salary_max:Q", title="Avg Salary (max)"),
        tooltip=["cluster_id", "cluster_name", "avg_salary_min", "avg_salary_max"]
    )
    .properties(height=350)
)

st.altair_chart(salary_chart, use_container_width=True)

# ============================================================
# VISUAL 3 — PIE CHART (Only Top 5 Labels)
# ============================================================
st.markdown("### 🥧 Cluster Share of Job Market")

summary_df["percent"] = (summary_df["job_count"] / summary_df["job_count"].sum() * 100).round(1)

# ---- FIX: Show labels only for top 5 clusters ----
top5 = summary_df.nlargest(5, "job_count")
summary_df["label"] = summary_df.apply(
    lambda row: f"{row['percent']}%" if row["cluster_id"] in top5["cluster_id"].values else "",
    axis=1
)

pie = (
    alt.Chart(summary_df)
    .mark_arc(outerRadius=120)
    .encode(
        theta=alt.Theta("job_count:Q"),
        color=alt.Color("cluster_id:N", title="Cluster ID"),
        tooltip=["cluster_id", "cluster_name", "job_count", "percent"]
    )
)



st.altair_chart(pie, use_container_width=True)

# import streamlit as st
# import pandas as pd
# import altair as alt
# from google.cloud import bigquery
# from openai import OpenAI

# # ============================================================
# # SETUP
# # ============================================================
# st.set_page_config(page_title="📊 AI Cluster Dashboard", layout="wide")
# st.title("📊 AI-Enhanced Job Cluster Dashboard")
# st.write("Automatically generated cluster insights using K-Means + GPT")

# client_bq = bigquery.Client()
# client_ai = OpenAI(api_key=st.secrets["OPEN_AI_API"])

# # ============================================================
# # LOAD CLUSTER SUMMARY
# # ============================================================
# @st.cache_data
# def load_cluster_summary():
#     query = """
#         SELECT
#           c.cluster_id,
#           COUNT(*) AS job_count,
#           ARRAY_AGG(j.title ORDER BY j.title LIMIT 20) AS titles,
#           AVG(j.salary_min) AS avg_salary_min,
#           AVG(j.salary_max) AS avg_salary_max
#         FROM `ba882-team4-474802.ba882_jobs.job_clusters` c
#         JOIN `ba882-team4-474802.ba882_jobs.jobs` j
#             ON c.job_id = j.job_id
#         GROUP BY c.cluster_id
#         ORDER BY c.cluster_id
#     """
#     return client_bq.query(query).to_dataframe()

# cluster_df = load_cluster_summary()

# if cluster_df.empty:
#     st.error("⚠ No cluster data available.")
#     st.stop()

# # ============================================================
# # AI-BASED CLUSTER NAMING (FILTERING OUT NON-DATA ROLES)
# # ============================================================
# def generate_cluster_name(titles):
#     prompt = f"""
#     You are an expert labor market analyst.
#     These job titles belong to a job cluster.
#     Your task:
#     - Generate a short cluster name (3–5 words)
#     - Ignore or filter out job titles unrelated to data, AI, analytics, cloud, engineering.
#     - Do NOT create names related to retail, healthcare, construction, music, or unrelated fields.

#     Titles:
#     {titles}

#     Output ONLY the cluster name.
#     """

#     response = client_ai.chat.completions.create(
#         model="gpt-4o-mini",
#         messages=[
#             {"role": "system", "content": "You generate names for data-related job clusters."},
#             {"role": "user", "content": prompt}
#         ]
#     )
#     return response.choices[0].message.content.strip()

# cluster_df["cluster_name"] = cluster_df["titles"].apply(lambda t: generate_cluster_name(t))

# # ============================================================
# # SUMMARY TABLE (Hide cluster_id)
# # ============================================================
# st.subheader("📌 Cluster Summary (AI-Generated Names Included)")

# summary_df = cluster_df[[
#     "cluster_id",
#     "cluster_name",
#     "job_count",
#     "avg_salary_min",
#     "avg_salary_max"
# ]]

# st.dataframe(
#     summary_df.drop(columns=["cluster_id"]),  # hide ID
#     use_container_width=True
# )

# # ============================================================
# # VISUAL 1 — JOB COUNT BY CLUSTER (SHOW cluster_id)
# # ============================================================
# st.markdown("### 📈 Job Count per Cluster")

# chart_count = (
#     alt.Chart(summary_df)
#     .mark_bar(color="#4BA3C3")
#     .encode(
#         x=alt.X("cluster_id:O", title="Cluster ID", axis=alt.Axis(labelAngle=0)),
#         y=alt.Y("job_count:Q", title="Number of Jobs"),
#         tooltip=["cluster_id", "cluster_name", "job_count"]
#     )
#     .properties(height=350)
# )

# st.altair_chart(chart_count, use_container_width=True)

# # ============================================================
# # VISUAL 2 — SALARY COMPARISON (SHOW cluster_id)
# # ============================================================
# st.markdown("### 💰 Average Salary by Cluster")

# salary_chart = (
#     alt.Chart(summary_df)
#     .mark_bar(color="#8BC34A")
#     .encode(
#         x=alt.X("cluster_id:O", title="Cluster ID", axis=alt.Axis(labelAngle=0)),
#         y=alt.Y("avg_salary_max:Q", title="Avg Salary (max)"),
#         tooltip=["cluster_id", "cluster_name", "avg_salary_min", "avg_salary_max"]
#     )
#     .properties(height=350)
# )

# st.altair_chart(salary_chart, use_container_width=True)

# # ============================================================
# # VISUAL 3 — PIE CHART WITH CLEAN PERCENT LABELS
# # ============================================================
# st.markdown("### 🥧 Cluster Share of Job Market")

# summary_df["percent"] = (summary_df["job_count"] / summary_df["job_count"].sum() * 100).round(1)

# pie = (
#     alt.Chart(summary_df)
#     .mark_arc(outerRadius=120)
#     .encode(
#         theta=alt.Theta("job_count:Q"),
#         color=alt.Color("cluster_id:N", title="Cluster ID"),
#         tooltip=["cluster_id", "cluster_name", "job_count", "percent"]
#     )
# )

# text = (
#     alt.Chart(summary_df)
#     .mark_text(radius=150, size=14, color="white", fontWeight="bold")
#     .encode(
#         theta="job_count:Q",
#         text=alt.Text("percent:Q", format=".1f")
#     )
# )

# st.altair_chart(pie + text, use_container_width=True)
