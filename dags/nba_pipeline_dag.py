"""
Airflow DAG: NBA ETL Pipeline.

Runs Bronze → Silver → Gold for a single game date.
- Daily: Automatically processes "yesterday's" games at midnight UTC.
- Backfill: Run `airflow dags backfill` from the CLI for 20-year history.

IMPORTANT:
  max_active_runs=1 prevents parallel API calls that would trigger rate limits.
  catchup=False means only today's run is scheduled — history is loaded via CLI backfill.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "data-eng",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

# Shared env so Airflow workers can find the nba_etl package
_PYTHONPATH_ENV = {"PYTHONPATH": "/opt/airflow/plugins"}

with DAG(
    dag_id="nba_etl_pipeline",
    description="NBA Bronze → Silver → Gold daily pipeline",
    default_args=default_args,
    start_date=datetime(2023, 10, 24),  # 2023-24 season opener
    schedule_interval="@daily",
    catchup=False,           # History handled via CLI backfill (see README)
    max_active_runs=1,       # CRITICAL: prevent API rate-limit bans
    tags=["nba", "etl"],
) as dag:

    # {{ ds }} = Airflow execution date (the day being processed, not "now")
    # For a daily DAG that runs at midnight, {{ ds }} = yesterday's date.

    ingest = BashOperator(
        task_id="bronze_ingest",
        bash_command="python -m nba_etl.bronze.ingestion {{ ds }}",
        env=_PYTHONPATH_ENV,
    )

    extract = BashOperator(
        task_id="silver_extract",
        bash_command="python -m nba_etl.silver.extraction {{ ds }}",
        env=_PYTHONPATH_ENV,
    )

    load = BashOperator(
        task_id="gold_load",
        bash_command="python -m nba_etl.gold.loading {{ ds }}",
        env=_PYTHONPATH_ENV,
    )

    ingest >> extract >> load


# ── Dimension Refresh (weekly) ────────────────────────────────────────
# Separate DAG that refreshes dimension tables (teams, players, games).
# Runs once a week on Monday to pick up roster moves, new teams, etc.

with DAG(
    dag_id="nba_etl_dims_refresh",
    description="Weekly refresh of NBA dimension tables",
    default_args=default_args,
    start_date=datetime(2023, 10, 24),
    schedule_interval="@weekly",
    catchup=False,
    max_active_runs=1,
    tags=["nba", "etl", "dimensions"],
) as dims_dag:

    refresh_dims = BashOperator(
        task_id="refresh_all_dims",
        bash_command=(
            "python -m nba_etl.bronze.ingestion {{ ds }} --dims && "
            "python -m nba_etl.silver.extraction {{ ds }} --dims && "
            "python -m nba_etl.gold.loading {{ ds }} --dims"
        ),
        env=_PYTHONPATH_ENV,
    )
