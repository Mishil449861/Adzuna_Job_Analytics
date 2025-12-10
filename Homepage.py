import streamlit as st

# Page configuration
st.set_page_config(
    page_title="JobIntel: U.S. Job Market Dashboard",
    page_icon="💼",
    layout="wide"
)

# ---------- Main Introduction Section ----------
st.markdown("""
# 📊 Job Market Analysis with Adzuna  
**Team 4 · Fall 2025**

Welcome, a fully integrated analytics suite that blends **real job market data** with **AI-powered insights**.  
Explore U.S. job postings, discover hiring trends, analyze clusters, compare salary benchmarks, and even match your resume to real job opportunities.

---

## 🚀 Platform Features

### 📊 **1. Job Market Dashboard**
- U.S. job market overview (volume, salary, roles, categories)  
- Weekly posting trends  
- Location-based job distributions  
- Top job categories  
- Company & category-level insights  

---

### 🔍 **2. Job Explorer**
- Search jobs by keyword, company, or category  
- View full job descriptions  
- Company-wide hiring analytics  
- Category-level title frequency & keyword cloud  

---

### 🏢 **3. Recruiter Insights Dashboard**
- Role & state–based hiring analysis  
- Top hiring companies & job posting volumes  
- Salary benchmarks with percentiles + box plot  
- Detailed job listing tables with links  

---

### 🤖 **4. AI Cluster Dashboard**
- K-Means clustering over U.S. job postings  
- AI-generated cluster names using GPT  
- Salary comparisons across clusters  
- Market share visualization (cluster pie chart)  

---

### 📄 **5. AI Resume Matcher + Hiring Assistant**
- Upload your resume (PDF/DOCX/TXT)  
- AI-extracted keywords (safe JSON parsing)  
- Embedding-based job similarity matching  
- **State-filtered job recommendations**  
- RAG-powered Hiring Assistant to draft job descriptions  

---

## 🛠️ Tools & Technology
- **Google BigQuery** for scalable job data storage  
- **Streamlit** for interactive dashboards  
- **OpenAI GPT Models** for cluster naming, resume parsing, hiring assistant  
- **Adzuna API** for U.S. job posting data  
- **Python + Altair + NLTK** for visualization & text processing  

---

### 🌟 Start exploring using the left sidebar navigation!
""")

# import streamlit as st

# # Page configuration: set browser tab title, icon, and layout
# st.set_page_config(
#     page_title="JobIntel: U.S. Job Market Dashboard",
#     page_icon="💼",
#     layout="wide"
# )

# # ---------- Main Introduction Section ----------
# st.markdown("""
# # 📊 Job Market Analysis with Adzuna
# **Team 4 · Fall 2025**

# This interactive dashboard explores recent U.S. job postings using data from the **Adzuna API**, stored in **BigQuery**, and visualized via **Streamlit**.

# ---

# ### 🛠️ Tools Used:
# - Google BigQuery  
# - Streamlit  
# - GCP Cloud Shell  
# - GitHub  

# ---

# ### 🔍 What You Can Explore:
# - U.S. job market overview (volume, salary, popular roles)  
# - Weekly posting trends by industry  
# - Top hiring companies  
# - Role insights by job category  
# - Location-based job distributions  
# - Category-wise keyword & job title visualizations  

# """)
