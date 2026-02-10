"""
Configuration settings for NBA ETL Pipeline
"""
import os

# Database Configuration
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432'),
    'database': os.getenv('DB_NAME', 'nba_stats'),
    'user': os.getenv('DB_USER', 'user'),
    'password': os.getenv('DB_PASSWORD', 'password')
}

# API Configuration
API_CONFIG = {
    'rate_limit_seconds': 2,  # Sleep time between API calls
    'max_retries': 3,
    'retry_delay': 5
}

# ETL Configuration
ETL_CONFIG = {
    'start_season': 2006,
    'end_season': 2025,
    'data_dir': './data',
    'raw_dir': './data/raw',
    'processed_dir': './data/processed',
    'logs_dir': './logs'
}

# Season Configuration
SEASON_TYPES = {
    'regular': 'Regular Season',
    'playoffs': 'Playoffs',
    'preseason': 'Pre Season'
}

# Database Table Names
TABLES = {
    'games': 'games',
    'teams': 'teams',
    'players': 'players',
    'game_stats': 'game_stats',
    'player_stats': 'player_game_stats',
    'team_stats': 'team_game_stats'
}