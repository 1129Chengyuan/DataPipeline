#!/bin/bash
# Creates the 'airflow' database for Airflow metadata.
# Runs automatically on first Postgres startup via docker-entrypoint-initdb.d.
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE DATABASE airflow;
EOSQL
