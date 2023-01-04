FROM apache/airflow:2.3.0
USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*
USER airflow
COPY /requirements.txt /requirements.txt

RUN pip install --no-cache-dir --user google-api-python-client google-auth-httplib2  google-auth-oauthlib python-dotenv
RUN pip install --no-cache-dir --user  gspread gspread-dataframe fuzzywuzzy paramiko argparse mysql-connector-python SQLAlchemy