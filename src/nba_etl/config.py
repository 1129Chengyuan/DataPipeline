"""
Centralized configuration for the NBA ETL pipeline.

All environment variables and defaults live here.
Import from any script:
    from nba_etl.config import settings

Defaults point to Airflow paths (/opt/airflow/datalake/...).
The standalone `etl` container overrides via env vars to /app/datalake/...
"""

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Pipeline configuration, overridable via environment variables."""

    # ── Paths ──
    bronze_path: str = "/opt/airflow/datalake/bronze"
    silver_path: str = "/opt/airflow/datalake/silver"

    # ── API ──
    rate_limit: float = 0.600  # seconds between NBA API calls

    # ── Database ──
    db_user: str = "root"
    db_pass: str = "root"
    db_name: str = "mydatabase"
    db_host: str = "pgdatabase"
    db_port: str = "5432"

    @property
    def db_url(self) -> str:
        return (
            f"postgresql://{self.db_user}:{self.db_pass}"
            f"@{self.db_host}:{self.db_port}/{self.db_name}"
        )

    class Config:
        env_file = ".env"
        extra = "ignore"


settings = Settings()

