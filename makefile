.PHONY: help setup build up down restart logs clean ingest transform etl test

help: ## Show this help message
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Available targets:'
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-20s %s\n", $$1, $$2}'

setup: ## Initial setup - create directories and .env file
	@echo "Setting up NBA ETL Pipeline..."
	mkdir -p data/raw data/processed logs
	cp .env.example .env
	@echo "Setup complete! Edit .env file if needed."

build: ## Build Docker images
	docker-compose build

up: ## Start all services
	docker-compose up -d

down: ## Stop all services
	docker-compose down

restart: ## Restart all services
	docker-compose restart

logs: ## View logs from all services
	docker-compose logs -f

logs-etl: ## View ETL logs only
	docker-compose logs -f etl

logs-db: ## View database logs only
	docker-compose logs -f db

clean: ## Remove all containers, volumes, and data
	docker-compose down -v
	rm -rf data/raw/* data/processed/* logs/*

# ETL Pipeline Commands

ingest: ## Run ingestion phase only (Extract & Load)
	docker-compose run --rm etl python run_etl.py --ingest-only

transform: ## Run transformation phase only
	docker-compose run --rm etl python run_etl.py --transform-only

etl: ## Run full ETL pipeline (default: 2006-2025)
	docker-compose run --rm etl python run_etl.py

etl-recent: ## Run ETL for recent seasons only (2020-2025)
	docker-compose run --rm etl python run_etl.py --start-year 2020 --end-year 2025

etl-single: ## Run ETL for single season (2023-24)
	docker-compose run --rm etl python run_etl.py --start-year 2023 --end-year 2024

# Database Commands

db-shell: ## Open PostgreSQL shell
	docker-compose exec db psql -U user -d nba_stats

db-backup: ## Backup database
	docker-compose exec db pg_dump -U user nba_stats > backup_$$(date +%Y%m%d_%H%M%S).sql

db-restore: ## Restore database from backup (usage: make db-restore FILE=backup.sql)
	docker-compose exec -T db psql -U user nba_stats < $(FILE)

# Development Commands

install: ## Install Python dependencies locally
	pip install -r requirements.txt

test-connection: ## Test database connection
	docker-compose exec db pg_isready -U user -d nba_stats

shell-etl: ## Open shell in ETL container
	docker-compose run --rm etl /bin/bash

# Monitoring Commands

stats: ## Show database statistics
	docker-compose exec db psql -U user -d nba_stats -c "\
		SELECT 'teams' as table_name, COUNT(*) as row_count FROM teams \
		UNION ALL \
		SELECT 'games', COUNT(*) FROM games \
		UNION ALL \
		SELECT 'team_stats', COUNT(*) FROM team_stats \
		UNION ALL \
		SELECT 'players', COUNT(*) FROM players;"

disk-usage: ## Show disk usage of data volumes
	docker system df -v | grep nba