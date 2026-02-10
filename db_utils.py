"""
Database utility functions for NBA ETL Pipeline
"""
import psycopg2
from psycopg2.extras import execute_values
from sqlalchemy import create_engine, text
import pandas as pd
import logging
from config import DB_CONFIG, TABLES

logger = logging.getLogger(__name__)


class DatabaseManager:
    """Manages database connections and operations"""
    
    def __init__(self, config=None):
        self.config = config or DB_CONFIG
        self.engine = None
        self.connection = None
        
    def get_connection_string(self):
        """Generate SQLAlchemy connection string"""
        return f"postgresql://{self.config['user']}:{self.config['password']}@" \
               f"{self.config['host']}:{self.config['port']}/{self.config['database']}"
    
    def get_engine(self):
        """Get or create SQLAlchemy engine"""
        if self.engine is None:
            self.engine = create_engine(self.get_connection_string())
        return self.engine
    
    def get_connection(self):
        """Get raw psycopg2 connection"""
        if self.connection is None or self.connection.closed:
            self.connection = psycopg2.connect(
                host=self.config['host'],
                port=self.config['port'],
                database=self.config['database'],
                user=self.config['user'],
                password=self.config['password']
            )
        return self.connection
    
    def close(self):
        """Close all connections"""
        if self.connection and not self.connection.closed:
            self.connection.close()
        if self.engine:
            self.engine.dispose()
    
    def create_tables(self):
        """Create all necessary tables for the NBA pipeline"""
        logger.info("Creating database tables...")
        
        sql_statements = [
            # Teams table
            f"""
            CREATE TABLE IF NOT EXISTS {TABLES['teams']} (
                team_id BIGINT PRIMARY KEY,
                team_abbreviation VARCHAR(10),
                team_name VARCHAR(100),
                team_city VARCHAR(100),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """,
            
            # Games table
            f"""
            CREATE TABLE IF NOT EXISTS {TABLES['games']} (
                game_id VARCHAR(20) PRIMARY KEY,
                season VARCHAR(10) NOT NULL,
                season_type VARCHAR(20),
                game_date DATE,
                home_team_id BIGINT,
                away_team_id BIGINT,
                home_team_score INTEGER,
                away_team_score INTEGER,
                game_status VARCHAR(20),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (home_team_id) REFERENCES {TABLES['teams']}(team_id),
                FOREIGN KEY (away_team_id) REFERENCES {TABLES['teams']}(team_id)
            );
            """,
            
            # Players table
            f"""
            CREATE TABLE IF NOT EXISTS {TABLES['players']} (
                player_id BIGINT PRIMARY KEY,
                player_name VARCHAR(100),
                team_id BIGINT,
                position VARCHAR(10),
                jersey_number VARCHAR(10),
                height VARCHAR(10),
                weight VARCHAR(10),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (team_id) REFERENCES {TABLES['teams']}(team_id)
            );
            """,
            
            # Team Game Stats table
            f"""
            CREATE TABLE IF NOT EXISTS {TABLES['team_stats']} (
                id SERIAL PRIMARY KEY,
                game_id VARCHAR(20) NOT NULL,
                team_id BIGINT NOT NULL,
                matchup VARCHAR(50),
                wl VARCHAR(1),
                pts INTEGER,
                fgm INTEGER,
                fga INTEGER,
                fg_pct DECIMAL(5,3),
                fg3m INTEGER,
                fg3a INTEGER,
                fg3_pct DECIMAL(5,3),
                ftm INTEGER,
                fta INTEGER,
                ft_pct DECIMAL(5,3),
                oreb INTEGER,
                dreb INTEGER,
                reb INTEGER,
                ast INTEGER,
                stl INTEGER,
                blk INTEGER,
                tov INTEGER,
                pf INTEGER,
                plus_minus INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (game_id) REFERENCES {TABLES['games']}(game_id),
                FOREIGN KEY (team_id) REFERENCES {TABLES['teams']}(team_id),
                UNIQUE(game_id, team_id)
            );
            """,
            
            # Player Game Stats table
            f"""
            CREATE TABLE IF NOT EXISTS {TABLES['player_stats']} (
                id SERIAL PRIMARY KEY,
                game_id VARCHAR(20) NOT NULL,
                player_id BIGINT NOT NULL,
                team_id BIGINT NOT NULL,
                player_name VARCHAR(100),
                start_position VARCHAR(10),
                comment VARCHAR(50),
                min VARCHAR(10),
                fgm INTEGER,
                fga INTEGER,
                fg_pct DECIMAL(5,3),
                fg3m INTEGER,
                fg3a INTEGER,
                fg3_pct DECIMAL(5,3),
                ftm INTEGER,
                fta INTEGER,
                ft_pct DECIMAL(5,3),
                oreb INTEGER,
                dreb INTEGER,
                reb INTEGER,
                ast INTEGER,
                stl INTEGER,
                blk INTEGER,
                tov INTEGER,
                pf INTEGER,
                pts INTEGER,
                plus_minus INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (game_id) REFERENCES {TABLES['games']}(game_id),
                FOREIGN KEY (player_id) REFERENCES {TABLES['players']}(player_id),
                FOREIGN KEY (team_id) REFERENCES {TABLES['teams']}(team_id)
            );
            """,
            
            # Create indexes
            f"CREATE INDEX IF NOT EXISTS idx_games_season ON {TABLES['games']}(season);",
            f"CREATE INDEX IF NOT EXISTS idx_games_date ON {TABLES['games']}(game_date);",
            f"CREATE INDEX IF NOT EXISTS idx_team_stats_game ON {TABLES['team_stats']}(game_id);",
            f"CREATE INDEX IF NOT EXISTS idx_player_stats_game ON {TABLES['player_stats']}(game_id);",
            f"CREATE INDEX IF NOT EXISTS idx_player_stats_player ON {TABLES['player_stats']}(player_id);",
        ]
        
        engine = self.get_engine()
        with engine.connect() as conn:
            for sql in sql_statements:
                try:
                    conn.execute(text(sql))
                    conn.commit()
                except Exception as e:
                    logger.error(f"Error executing SQL: {e}")
                    raise
        
        logger.info("Database tables created successfully")
    
    def insert_dataframe(self, df, table_name, if_exists='append'):
        """Insert a pandas DataFrame into a table"""
        engine = self.get_engine()
        df.to_sql(table_name, engine, if_exists=if_exists, index=False)
        logger.info(f"Inserted {len(df)} rows into {table_name}")
    
    def execute_query(self, query, params=None):
        """Execute a query and return results as DataFrame"""
        engine = self.get_engine()
        return pd.read_sql(query, engine, params=params)
    
    def bulk_insert(self, table_name, data, columns):
        """Bulk insert data using psycopg2 execute_values for better performance"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cols = ', '.join(columns)
        placeholders = ', '.join(['%s'] * len(columns))
        query = f"INSERT INTO {table_name} ({cols}) VALUES %s ON CONFLICT DO NOTHING"
        
        try:
            execute_values(cursor, query, data)
            conn.commit()
            logger.info(f"Bulk inserted {len(data)} rows into {table_name}")
        except Exception as e:
            conn.rollback()
            logger.error(f"Error in bulk insert: {e}")
            raise
        finally:
            cursor.close()
    
    def table_exists(self, table_name):
        """Check if a table exists"""
        query = """
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = %s
            );
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(query, (table_name,))
        exists = cursor.fetchone()[0]
        cursor.close()
        return exists
    
    def get_row_count(self, table_name):
        """Get the number of rows in a table"""
        query = f"SELECT COUNT(*) FROM {table_name}"
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(query)
        count = cursor.fetchone()[0]
        cursor.close()
        return count