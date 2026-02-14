# Airflow image with NBA ETL dependencies
FROM apache/airflow:2.8.1-python3.11

USER airflow

# Install the same dependencies as our ETL pipeline
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
