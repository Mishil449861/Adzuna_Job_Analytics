import streamlit as st
import pandas as pd
import altair as alt
from google.cloud import bigquery

# -----------------------------------------
# STREAMLIT PAGE SETUP
# -----------------------------------------
st.set_page_config(page_title="🏢 Recruiter Dashboard", layout="wide")
st.title("🏢 Recruiter Insights Dashboard")
st.write("Analyze hiring activity, job details, and salary benchmarks in one place.")

client = bigquery.Client()

# -----------------------------------------
# LOAD STATE LIST (dropdown)
# -----------------------------------------
@st.cache_data
def get_all_states():
    query = """
        SELECT DISTINCT state
        FROM `ba882-team4-474802.ba882_jobs.locations`
        WHERE state IS NOT NULL AND state != ''
        ORDER BY state
    """
    df = client.query(query).to_dataframe()
    return df["state"].tolist()

states = get_all_states()

# -----------------------------------------
# SIDEBAR FILTERS
# -----------------------------------------
st.sidebar.header("🔎 Filters")

role_input = st.sidebar.text_input("Role Keyword (e.g., Data, Engineer, Analyst)", "Data")
state_input = st.sidebar.selectbox("State", states)

# TABS
tab1, tab2 = st.tabs(["🏢 Top Hiring Companies", "💰 Salary Benchmark"])

# ============================================================
# TAB 1: TOP HIRING COMPANIES
# ============================================================
@st.cache_data
def load_top_hiring_companies(role, state):
    query = """
        SELECT
            j.company_name,
            COUNT(*) AS posting_count
        FROM `ba882-team4-474802.ba882_jobs.jobs` j
        WHERE
            LOWER(j.title) LIKE CONCAT('%', LOWER(@role), '%')
            AND LOWER(j.state) = LOWER(@state)
        GROUP BY company_name
        ORDER BY posting_count DESC
        LIMIT 15
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("role", "STRING", role),
            bigquery.ScalarQueryParameter("state", "STRING", state),
        ]
    )
    return client.query(query, job_config=job_config).to_dataframe()


@st.cache_data
def load_job_details(role, state, company):
    query = """
        SELECT
            j.title,
            j.company_name,
            j.salary_min,
            j.salary_max,
            (j.salary_min + j.salary_max) / 2 AS avg_salary,
            j.city,
            j.state,
            j.redirected_url,
            FORMAT_TIMESTAMP('%Y-%m-%d', j.created) AS created_date
        FROM `ba882-team4-474802.ba882_jobs.jobs` j
        WHERE
            LOWER(j.title) LIKE CONCAT('%', LOWER(@role), '%')
            AND LOWER(j.state) = LOWER(@state)
            AND j.company_name = @company
        ORDER BY created DESC
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("role", "STRING", role),
            bigquery.ScalarQueryParameter("state", "STRING", state),
            bigquery.ScalarQueryParameter("company", "STRING", company),
        ]
    )

    return client.query(query, job_config=job_config).to_dataframe()


with tab1:
    st.subheader(f"📊 Who Is Hiring roles containing '{role_input}' in {state_input}?")
    df = load_top_hiring_companies(role_input, state_input)

    if df.empty:
        st.warning("⚠️ No job postings found for the selected filters.")
    else:
        chart = (
            alt.Chart(df)
            .mark_bar(color="#4BA3C3")
            .encode(
                x=alt.X("company_name:N", sort="-y", title="Company"),
                y=alt.Y("posting_count:Q", title="Job Postings"),
                tooltip=["company_name", "posting_count"]
            )
            .properties(height=350)
        )
        st.altair_chart(chart, use_container_width=True)

        col1, col2, col3 = st.columns(3)
        col1.metric("Top Hiring Company", df.iloc[0]["company_name"])
        col2.metric("Jobs Posted by Top Company", int(df.iloc[0]["posting_count"]))
        col3.metric("Companies Hiring", df.shape[0])

        st.markdown("---")

        st.subheader("🏢 View Job Details by Company")
        selected_company = st.selectbox("Select a Company", df["company_name"].tolist())

        details_df = load_job_details(role_input, state_input, selected_company)
        if details_df.empty:
            st.info("No job details available for this company.")
        else:
            details_df["redirected_url"] = details_df["redirected_url"].apply(
                lambda x: f"[Link]({x})" if x else ""
            )
            st.dataframe(details_df, use_container_width=True)


# ============================================================
# TAB 2: SALARY BENCHMARK BOX PLOT
# ============================================================
@st.cache_data
def load_salary_data(role, state):
    query = """
        SELECT
            (salary_min + salary_max) / 2 AS avg_salary,
            salary_min,
            salary_max,
            title,
            company_name,
            city,
            state,
            redirected_url,
            FORMAT_TIMESTAMP('%Y-%m-%d', created) AS created_date
        FROM `ba882-team4-474802.ba882_jobs.jobs`
        WHERE
            salary_min IS NOT NULL
            AND salary_max IS NOT NULL
            AND LOWER(title) LIKE CONCAT('%', LOWER(@role), '%')
            AND LOWER(state) = LOWER(@state)
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("role", "STRING", role),
            bigquery.ScalarQueryParameter("state", "STRING", state),
        ]
    )
    return client.query(query, job_config=job_config).to_dataframe()


with tab2:
    st.subheader(f"💰 Salary Benchmark for roles matching '{role_input}' in {state_input}")

    salary_df = load_salary_data(role_input, state_input)

    if salary_df.empty:
        st.warning("⚠️ No salary data found for this role and state.")
    else:
        # Percentiles
        p25 = salary_df["avg_salary"].quantile(0.25)
        p50 = salary_df["avg_salary"].quantile(0.50)
        p75 = salary_df["avg_salary"].quantile(0.75)

        st.write("### 📌 Salary Percentiles")
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("P25", f"${int(p25):,}")
        c2.metric("Median (P50)", f"${int(p50):,}")
        c3.metric("P75", f"${int(p75):,}")
        c4.metric("Data Points", len(salary_df))

        # Box Plot
        chart = (
            alt.Chart(salary_df)
            .mark_boxplot(color="#6FB98F")
            .encode(
                y=alt.Y("avg_salary:Q", title="Average Salary"),
                tooltip=["avg_salary", "title", "company_name"]
            )
            .properties(height=300)
        )
        st.altair_chart(chart, use_container_width=True)

        st.markdown("---")

        # Raw Table
        st.write("### 📄 Raw Salary Data")
        salary_df["redirected_url"] = salary_df["redirected_url"].apply(
            lambda x: f"[Link]({x})" if x else ""
        )
        st.dataframe(salary_df, use_container_width=True)