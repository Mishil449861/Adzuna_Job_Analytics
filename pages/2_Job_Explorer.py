import streamlit as st
import pandas as pd
import altair as alt
from google.cloud import bigquery
from collections import Counter
import re
from wordcloud import WordCloud
import matplotlib.pyplot as plt
import nltk
from nltk.corpus import stopwords

# ✅ Setup
client = bigquery.Client()
st.set_page_config(page_title="Job Explorer", page_icon="🔍", layout="wide")
st.title("🔍 Job Explorer")
st.caption("Search jobs by keyword, company, or category.")

# ✅ Tab layout
tab1, tab2, tab3 = st.tabs(["🔎 Job Search", "🏢 Company Search", "📂 Category Search"])

# ──────────────────────────────────────────────────────────────
# 🔎 JOB TITLE SEARCH TAB
with tab1:
    keyword = st.text_input("🔎 Enter a keyword to search job titles:")

    @st.cache_data
    def search_jobs_by_keyword(kw):
        if not kw:
            return pd.DataFrame()
        query = """
            SELECT
                j.title,
                comp.company_name,
                cat.category_label,
                loc.city,
                loc.state,
                loc.country,
                j.description
            FROM `ba882-team4-474802.ba882_jobs.jobs` j
            LEFT JOIN `ba882-team4-474802.ba882_jobs.companies` comp 
                ON j.company_id = comp.company_id
            LEFT JOIN `ba882-team4-474802.ba882_jobs.categories` cat 
                ON j.category_id = cat.category_id
            LEFT JOIN `ba882-team4-474802.ba882_jobs.locations` loc 
                ON j.location_id = loc.location_id
            WHERE LOWER(j.title) LIKE @search
            LIMIT 100
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("search", "STRING", f"%{kw.lower()}%")]
        )
        return client.query(query, job_config=job_config).to_dataframe()

    if keyword:
        df = search_jobs_by_keyword(keyword)
        if df.empty:
            st.warning("⚠️ No jobs found for the given keyword.")
        else:
            st.markdown(f"#### Showing results for: **{keyword}**")
            display_cols = ["title", "company_name", "category_label", "city", "state", "country"]
            st.dataframe(df[display_cols])

            job_titles = df["title"] + " at " + df["company_name"]
            selected_job = st.selectbox("📌 Select a job to view full description:", job_titles)

            if selected_job:
                row = df[job_titles == selected_job].iloc[0]
                st.markdown("### 📝 Job Description")
                st.markdown(f"**Job Title:** {row['title']}")
                st.markdown(f"**Company:** {row['company_name']}")
                st.markdown(f"**Location:** {row['city']}, {row['state']}, {row['country']}")
                st.markdown(f"**Category:** {row['category_label']}")
                st.markdown("---")
                st.write(row["description"])
    else:
        st.info("💡 Please enter a keyword above to search job titles.")

# ──────────────────────────────────────────────────────────────
# 🏢 COMPANY-WISE SEARCH TAB
with tab2:
    @st.cache_data
    def get_company_list():
        query = """
            SELECT DISTINCT company_name
            FROM `ba882-team4-474802.ba882_jobs.companies`
            WHERE company_name IS NOT NULL
            ORDER BY company_name
        """
        df = client.query(query).to_dataframe()
        return df["company_name"].tolist()

    @st.cache_data
    def get_company_metrics(company):
        query = """
            SELECT
                j.title,
                comp.company_name,
                cat.category_label,
                j.salary_min,
                j.salary_max
            FROM `ba882-team4-474802.ba882_jobs.jobs` j
            LEFT JOIN `ba882-team4-474802.ba882_jobs.companies` comp
                ON j.company_id = comp.company_id
            LEFT JOIN `ba882-team4-474802.ba882_jobs.categories` cat
                ON j.category_id = cat.category_id
            WHERE comp.company_name = @company
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("company", "STRING", company)]
        )
        return client.query(query, job_config=job_config).to_dataframe()

    company_list = get_company_list()
    selected_company = st.selectbox("🏢 Select a company", company_list)

    company_df = get_company_metrics(selected_company)
    if company_df.empty:
        st.warning(f"No job postings found for {selected_company}.")
    else:
        total_postings = len(company_df)
        avg_salary = round(((company_df["salary_min"] + company_df["salary_max"]) / 2).mean())

        col1, col2 = st.columns(2)
        col1.metric("📌 Total Postings", f"{total_postings:,}")
        col2.metric("💰 Avg. Salary", f"${avg_salary:,}")

        st.subheader(f"📋 Job Titles at: {selected_company}")
        st.dataframe(company_df[["title", "category_label"]])

# ──────────────────────────────────────────────────────────────
# 📂 CATEGORY-WISE SEARCH TAB
with tab3:
    st.markdown("### 📂 Category-wise Exploration")
    st.caption("Explore the most common **job titles** and **tech keywords** within each job category.")

    # 🔄 Load available categories
    @st.cache_data
    def load_category_list():
        query = """
            SELECT DISTINCT category_label
            FROM `ba882-team4-474802.ba882_jobs.categories`
            WHERE category_label IS NOT NULL
            ORDER BY category_label
        """
        df = client.query(query).to_dataframe()
        return df["category_label"].tolist()

    # 🔄 Load job details for a category
    @st.cache_data
    def load_category_jobs(category):
        query = """
            SELECT 
                j.title, 
                j.description
            FROM `ba882-team4-474802.ba882_jobs.jobs` j
            LEFT JOIN `ba882-team4-474802.ba882_jobs.categories` cat
                ON j.category_id = cat.category_id
            WHERE cat.category_label = @category
            LIMIT 500
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("category", "STRING", category)]
        )
        return client.query(query, job_config=job_config).to_dataframe()

    # 카테고리 선택
    categories = load_category_list()
    selected_category = st.selectbox("📌 Select a job category", categories)

    if selected_category:
        df_jobs = load_category_jobs(selected_category)

        if df_jobs.empty:
            st.warning("No data available for this category.")
        else:
            # 🔹 상위 직무 title 시각화
            st.subheader("🏷️ Top Job Titles in Selected Category")

            title_counts = df_jobs["title"].value_counts().reset_index()
            title_counts.columns = ["title", "count"]
            top_titles = title_counts.head(10)

            chart = (
                alt.Chart(top_titles)
                .mark_bar(color="#7aa9ff")
                .encode(
                    x=alt.X("count:Q", title="Count", axis=alt.Axis(format="d")),
                    y=alt.Y("title:N", sort="-x", title="Job Title"),
                    tooltip=["title", "count"]
                )
                .properties(width=700, height=400)
            )
            st.altair_chart(chart, use_container_width=True)

            
        # NLTK stopwords 다운로드 (최초 1회만 필요)
            nltk.download('stopwords')

            st.subheader("🧠 Common Tech Keywords from Job Descriptions")

            # 1️⃣ 모든 job description 합치기
            all_text = " ".join(df_jobs["description"].dropna().tolist()).lower()

            # 2️⃣ 단어 토큰화 (4자 이상 영어 단어만)
            tokens = re.findall(r'\b[a-z]{4,}\b', all_text)

            # 3️⃣ NLTK 내장 불용어 + 도메인 관련 추가 stopwords
            stop_words = set(stopwords.words('english'))
            custom_stopwords = set([
                "team", "role", "skills", "experience", "knowledge", "position",
                "responsible", "requirements", "preferred", "working", "years",
                "ability", "work", "using", "must", "include", "will", "etc",
                "description", "company", "environment", "strong", "good"
            ])
            stop_words.update(custom_stopwords)

            # 4️⃣ stopwords 제외
            filtered_tokens = [t for t in tokens if t not in stop_words]

            # 5️⃣ 단어 빈도 계산
            keyword_freq = Counter(filtered_tokens)

            # 6️⃣ Word Cloud 시각화
            wordcloud = WordCloud(
                width=800,
                height=400,
                background_color='white',
                max_words=100
            ).generate_from_frequencies(keyword_freq)

            fig, ax = plt.subplots(figsize=(10, 4))
            ax.imshow(wordcloud, interpolation='bilinear')
            ax.axis('off')
            st.pyplot(fig)