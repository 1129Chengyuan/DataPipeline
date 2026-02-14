"""
Airflow DAG: NBA ETL Pipeline.

Runs Bronze → Silver → Gold for a single game date.
Designed for daily execution with backfill support.

Prerequisites:
    - The nba_etl package must be on PYTHONPATH.
    - PostgreSQL must be accessible from the Airflow worker.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "data-eng",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="nba_etl_pipeline",
    description="NBA Bronze → Silver → Gold daily pipeline",
    default_args=default_args,
    start_date=datetime(2024, 10, 22),   # 2024-25 season opener
    schedule_interval="@daily",
    catchup=False,
    tags=["nba", "etl"],
) as dag:

    # {{ ds }} is Airflow's execution date in YYYY-MM-DD format
    ingest = BashOperator(
        task_id="bronze_ingest",
        bash_command="python -m nba_etl.bronze.ingestion {{ ds }}",
    )

    extract = BashOperator(
        task_id="silver_extract",
        bash_command="python -m nba_etl.silver.extraction {{ ds }}",
    )

    load = BashOperator(
        task_id="gold_load",
        bash_command="python -m nba_etl.gold.loading {{ ds }}",
    )

    # Dimensions refreshed weekly on Mondays
    refresh_dims = BashOperator(
        task_id="refresh_dimensions",
        bash_command=(
            "python -m nba_etl.bronze.ingestion {{ ds }} --dims && "
            "python -m nba_etl.silver.extraction {{ ds }} --dims && "
            "python -m nba_etl.gold.loading {{ ds }} --dims"
        ),
    )

    ingest >> extract >> load
