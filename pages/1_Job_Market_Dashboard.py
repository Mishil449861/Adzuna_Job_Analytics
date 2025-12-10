import streamlit as st
import pandas as pd
import altair as alt
from google.cloud import bigquery

# Setup
st.set_page_config(page_title="📊 Job Market Dashboard", layout="wide")
st.title("📊 Job Market Dashboard")

# BigQuery client
client = bigquery.Client()

# ===============================
# CACHED QUERIES
# ===============================

@st.cache_data
def get_overview_metrics():
    query = """
        WITH combined AS (
            SELECT
                j.job_id,
                comp.company_name,
                cat.category_label,
                j.salary_min,
                j.salary_max,
                j.title
            FROM `ba882-team4-474802.ba882_jobs.jobs` j
            LEFT JOIN `ba882-team4-474802.ba882_jobs.companies` comp
              ON j.company_id = comp.company_id
            LEFT JOIN `ba882-team4-474802.ba882_jobs.categories` cat
              ON j.category_id = cat.category_id
        )
        SELECT
            COUNT(DISTINCT job_id) AS total_jobs,
            COUNT(DISTINCT company_name) AS unique_companies,
            COUNT(DISTINCT category_label) AS total_categories,
            ROUND(AVG((salary_min + salary_max) / 2), 0) AS avg_salary,
            (SELECT title FROM combined GROUP BY title ORDER BY COUNT(*) DESC LIMIT 1) AS most_common_title
        FROM combined
    """
    return client.query(query).to_dataframe()

@st.cache_data
def get_weekly_trends():
    query = """
        SELECT
            FORMAT_DATE('%Y-%m-%d', DATE_TRUNC(DATE(created), WEEK(MONDAY))) AS week_start,
            COUNT(*) AS job_count
        FROM `ba882-team4-474802.ba882_jobs.jobs`
        WHERE created IS NOT NULL
        GROUP BY week_start
        ORDER BY week_start
    """
    return client.query(query).to_dataframe()

@st.cache_data
def get_city_insights(selected_category=None):
    query = """
        SELECT
            loc.city,
            COUNT(j.job_id) AS job_count,
            ROUND(AVG((j.salary_min + j.salary_max) / 2), 0) AS avg_salary
        FROM `ba882-team4-474802.ba882_jobs.jobs` j
        LEFT JOIN `ba882-team4-474802.ba882_jobs.locations` loc
            ON j.location_id = loc.location_id
        LEFT JOIN `ba882-team4-474802.ba882_jobs.categories` cat
            ON j.category_id = cat.category_id
        WHERE loc.city IS NOT NULL
        AND (@category IS NULL OR cat.category_label = @category)
        GROUP BY loc.city
        ORDER BY job_count DESC
        LIMIT 10
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("category", "STRING", selected_category)]
    )
    return client.query(query, job_config=job_config).to_dataframe()

@st.cache_data
def get_all_categories():
    query = """
        SELECT DISTINCT category_label
        FROM `ba882-team4-474802.ba882_jobs.categories`
        ORDER BY category_label
    """
    df = client.query(query).to_dataframe()
    return df["category_label"].tolist()

@st.cache_data
def load_category_data():
    query = """
        SELECT
            cat.category_label,
            COUNT(*) AS job_count
        FROM `ba882-team4-474802.ba882_jobs.jobs` j
        LEFT JOIN `ba882-team4-474802.ba882_jobs.categories` cat
            ON j.category_id = cat.category_id
        WHERE cat.category_label IS NOT NULL
        GROUP BY cat.category_label
        ORDER BY job_count DESC
    """
    return client.query(query).to_dataframe()

@st.cache_data
def load_jobs_by_category(category):
    query = """
        SELECT 
            j.title, 
            comp.company_name, 
            j.salary_min, 
            j.salary_max
        FROM `ba882-team4-474802.ba882_jobs.jobs` j
        LEFT JOIN `ba882-team4-474802.ba882_jobs.categories` cat
            ON j.category_id = cat.category_id
        LEFT JOIN `ba882-team4-474802.ba882_jobs.companies` comp
            ON j.company_id = comp.company_id
        WHERE cat.category_label = @category
        LIMIT 100
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("category", "STRING", category)]
    )
    return client.query(query, job_config=job_config).to_dataframe()

# ===============================
# TAB LAYOUT
# ===============================

tab1, tab2, tab3, tab4 = st.tabs(["📌 Overview", "📈 Weekly Trends", "📍 Location Insights", "📂 Top Categories"])

# -------------------------------
# 📌 Tab 1 - Overview
# -------------------------------
with tab1:
    st.subheader("📌 Job Market Overview")
    df = get_overview_metrics()
    col1, col2, col3 = st.columns(3)
    col1.metric("📌 Total Jobs Posted", f"{int(df['total_jobs'][0]):,}")
    col2.metric("🏢 Unique Companies Hiring", f"{int(df['unique_companies'][0]):,}")
    col3.metric("🗂️ Categories", f"{int(df['total_categories'][0]):,}")

    col4, col5 = st.columns(2)
    col4.metric("💰 Average Salary", f"${int(df['avg_salary'][0]):,}")
    col5.metric("🔥 Most Popular Job Title", df['most_common_title'][0])

# -------------------------------
# 📈 Tab 2 - Weekly Trends
# -------------------------------
with tab2:
    st.subheader("📈 Weekly Job Posting Trends")
    df = get_weekly_trends()
    if df.empty:
        st.warning("⚠️ No data found for weekly job postings.")
    else:
        chart = (
            alt.Chart(df)
            .mark_line(point=True, color="#007ACC")
            .encode(
                x=alt.X("week_start:T", title="Week Starting"),
                y=alt.Y("job_count:Q", title="Number of Job Postings"),
                tooltip=["week_start", "job_count"]
            )
            .properties(height=400)
        )
        st.altair_chart(chart, use_container_width=True)
        col1, col2 = st.columns(2)
        col1.metric("🗓️ Weeks Tracked", df.shape[0])
        col2.metric("📌 Avg. Jobs / Week", f"{int(df['job_count'].mean()):,}")

# -------------------------------
# 📍 Tab 3 - Location Insights
# -------------------------------
with tab3:
    st.subheader("📍 Location-Based Job Insights")
    categories = get_all_categories()
    selected = st.selectbox("🔎 Filter by Category", ["All"] + categories)
    cat_filter = None if selected == "All" else selected
    df = get_city_insights(cat_filter)

    if df.empty:
        st.warning("⚠️ No data available for this category.")
    else:
        chart = (
            alt.Chart(df)
            .mark_bar(color="#4BA3C3")
            .encode(
                x=alt.X("city:N", sort="-y"),
                y=alt.Y("job_count:Q"),
                tooltip=["city", "job_count", "avg_salary"]
            )
            .properties(height=400)
        )
        st.altair_chart(chart, use_container_width=True)

        col1, col2, col3 = st.columns(3)
        top_city = df.loc[df["avg_salary"].idxmax()]
        col1.metric("Highest Paying City", top_city["city"], f"${int(top_city['avg_salary']):,}")
        col2.metric("Average Across Top Cities", f"${int(df['avg_salary'].mean()):,}")
        col3.metric("Category Filter", selected if selected != "All" else "None")

# -------------------------------
# 📂 Tab 4 - Top Categories
# -------------------------------
with tab4:
    st.subheader("📂 Top Categories by Job Count")
    df_cat = load_category_data()
    top5_df = df_cat.head(5)
    chart = (
        alt.Chart(top5_df)
        .mark_bar(color="#66b3ff")
        .encode(
            x=alt.X("category_label:N", sort="-y", axis=alt.Axis(labelAngle=-25)),
            y=alt.Y("job_count:Q"),
            tooltip=["category_label", "job_count"]
        )
        .properties(width=700, height=400)
    )
    st.altair_chart(chart, use_container_width=True)

    selected_category = st.selectbox("🔎 Select a Category", df_cat["category_label"])
    if selected_category:
        jobs_df = load_jobs_by_category(selected_category)
        st.subheader(f"🗂️ Jobs under: {selected_category}")
        st.dataframe(jobs_df)