import os
import time
import pandas as pd
import traceback
from sqlalchemy import create_engine, text
from nba_api.stats.static import teams, players
from nba_api.stats.endpoints import leaguegamefinder

# Database configuration from environment variables
DB_USER = os.getenv('DB_USER', 'root')
DB_PASS = os.getenv('DB_PASS', 'root')
DB_NAME = os.getenv('DB_NAME', 'mydatabase')
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')

DATABASE_URL = f'postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}'

def get_engine():
    return create_engine(DATABASE_URL)

def init_db(engine):
    """Create tables if they don't already exist (idempotent)."""
    print("Initializing database...")
    with open('schema.sql', 'r') as f:
        schema_sql = f.read()
    with engine.connect() as conn:
        conn.execute(text(schema_sql))
        conn.commit()
    print("Database initialized.")

def extract_teams():
    print("Extracting teams...")
    all_teams = teams.get_teams()
    return pd.DataFrame(all_teams)

def extract_players():
    print("Extracting players...")
    all_players = players.get_players()
    return pd.DataFrame(all_players)

def extract_games(team_ids):
    print(f"Extracting games for {len(team_ids)} teams...")
    all_games = []
    for team_id in team_ids:
        gamefinder = leaguegamefinder.LeagueGameFinder(team_id_nullable=team_id)
        games_df = gamefinder.get_data_frames()[0]
        all_games.append(games_df)
        time.sleep(1)  # Rate limiting
    return pd.concat(all_games)

def transform_data(teams_df, players_df, games_df):
    print("Transforming data...")
    # Teams
    teams_df = teams_df[['id', 'full_name', 'abbreviation', 'nickname', 'city', 'state', 'year_founded']]
    
    # Players
    players_df = players_df[['id', 'full_name', 'first_name', 'last_name', 'is_active']]
    
    # Games
    games_df = games_df[['GAME_ID', 'TEAM_ID', 'GAME_DATE', 'MATCHUP', 'WL', 'PTS', 'AST', 'REB']]
    games_df.columns = [col.lower() for col in games_df.columns]
    
    # Cleaning
    games_df = games_df.dropna(subset=['game_id', 'team_id'])
    games_df = games_df.drop_duplicates(subset=['game_id', 'team_id'])
    
    games_df['game_date'] = pd.to_datetime(games_df['game_date'])
    
    return teams_df, players_df, games_df

def _upsert_via_staging(engine, df, target_table, conflict_cols, update_cols):
    """
    Load a DataFrame into a temp staging table, then merge into the
    target table using INSERT ... ON CONFLICT ... DO UPDATE SET.
    """
    staging_table = f"{target_table}_staging"
    
    with engine.connect() as conn:
        # Drop staging table if it exists from a previous failed run
        conn.execute(text(f"DROP TABLE IF EXISTS {staging_table}"))
        conn.commit()
    
    # Load data into the staging table
    df.to_sql(staging_table, engine, if_exists='replace', index=False, method='multi')
    
    # Build the upsert SQL
    all_cols = df.columns.tolist()
    col_list = ', '.join(all_cols)
    conflict_list = ', '.join(conflict_cols)
    update_set = ', '.join(
        [f"{col} = EXCLUDED.{col}" for col in update_cols]
    )
    
    upsert_sql = f"""
        INSERT INTO {target_table} ({col_list})
        SELECT {col_list} FROM {staging_table}
        ON CONFLICT ({conflict_list})
        DO UPDATE SET {update_set};
    """
    
    with engine.connect() as conn:
        conn.execute(text(upsert_sql))
        conn.execute(text(f"DROP TABLE IF EXISTS {staging_table}"))
        conn.commit()

def load_data(engine, teams_df, players_df, games_df):
    """Load data using idempotent upserts (INSERT ... ON CONFLICT)."""
    print("Loading data into Postgres...")
    try:
        # Upsert teams
        print("  Upserting teams...")
        _upsert_via_staging(
            engine, teams_df, 'teams',
            conflict_cols=['id'],
            update_cols=['full_name', 'abbreviation', 'nickname', 'city', 'state', 'year_founded']
        )
        
        # Upsert players
        print("  Upserting players...")
        _upsert_via_staging(
            engine, players_df, 'players',
            conflict_cols=['id'],
            update_cols=['full_name', 'first_name', 'last_name', 'is_active']
        )
        
        # Upsert games
        if not games_df.empty:
            print(f"  Upserting {len(games_df)} game records...")
            _upsert_via_staging(
                engine, games_df, 'games',
                conflict_cols=['game_id', 'team_id'],
                update_cols=['game_date', 'matchup', 'wl', 'pts', 'ast', 'reb']
            )
        
        print("Data loaded successfully.")
    except Exception as e:
        print(f"Error loading data: {e}")
        traceback.print_exc()
        raise

def run_pipeline():
    engine = get_engine()
    
    # Wait for DB to be ready
    retries = 5
    while retries > 0:
        try:
            init_db(engine)
            break
        except Exception as e:
            print(f"Database not ready, retrying in 5s... ({e})")
            retries -= 1
            time.sleep(5)
    
    teams_df = extract_teams()
    players_df = extract_players()
    
    # Use IDs from teams_df to fetch games
    team_ids = teams_df['id'].tolist()
    games_df = extract_games(team_ids)
    
    teams_df, players_df, games_df = transform_data(teams_df, players_df, games_df)
    
    load_data(engine, teams_df, players_df, games_df)

if __name__ == "__main__":
    run_pipeline()
