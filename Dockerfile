FROM astrocrpublic.azurecr.io/runtime:3.1-3
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY include/ba882-team4-474802-bee53a65f2ac.json /usr/local/airflow/include/