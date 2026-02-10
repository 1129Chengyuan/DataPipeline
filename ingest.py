"""
NBA Data Ingestion Script
Extracts NBA game data from the nba_api and loads it into PostgreSQL
"""

import time
import json
import os
import logging
from datetime import datetime
import pandas as pd
from nba_api.stats.endpoints import leaguegamefinder, boxscoretraditionalv2
from nba_api.stats.static import teams as static_teams
from nba_api.stats.static import players as static_players

from config import API_CONFIG, ETL_CONFIG, SEASON_TYPES, TABLES
from db_utils import DatabaseManager

# Setup logging
os.makedirs(ETL_CONFIG['logs_dir'], exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"{ETL_CONFIG['logs_dir']}/ingest.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class NBADataIngester:
    """Handles extraction of NBA data from the API"""
    
    def __init__(self, db_manager):
        self.db = db_manager
        self.rate_limit = API_CONFIG['rate_limit_seconds']
        
        # Create data directories
        os.makedirs(ETL_CONFIG['data_dir'], exist_ok=True)
        os.makedirs(ETL_CONFIG['raw_dir'], exist_ok=True)
        os.makedirs(ETL_CONFIG['processed_dir'], exist_ok=True)
    
    def generate_season_strings(self, start_year, end_year):
        """Generate NBA season strings"""
        seasons = []
        for year in range(start_year, end_year + 1):
            next_year = str(year + 1)[-2:]
            season_str = f"{year}-{next_year}"
            seasons.append(season_str)
        return seasons
    
    def extract_teams(self):
        """Extract all NBA teams and load into database"""
        logger.info("Extracting teams data...")
        
        try:
            teams = static_teams.get_teams()
            df_teams = pd.DataFrame(teams)
            
            # Rename columns to match our schema
            df_teams = df_teams.rename(columns={
                'id': 'team_id',
                'abbreviation': 'team_abbreviation',
                'full_name': 'team_name',
                'city': 'team_city'
            })
            
            # Select only the columns we need
            df_teams = df_teams[['team_id', 'team_abbreviation', 'team_name', 'team_city']]
            
            # Load to database
            self.db.insert_dataframe(df_teams, TABLES['teams'], if_exists='replace')
            
            logger.info(f"Loaded {len(df_teams)} teams into database")
            return df_teams
            
        except Exception as e:
            logger.error(f"Error extracting teams: {e}")
            raise
    
    def extract_players(self):
        """Extract all NBA players and load into database"""
        logger.info("Extracting players data...")
        
        try:
            players = static_players.get_players()
            df_players = pd.DataFrame(players)
            
            # Rename columns
            df_players = df_players.rename(columns={
                'id': 'player_id',
                'full_name': 'player_name'
            })
            
            # Select columns (note: static API doesn't have team_id, will be updated from game stats)
            df_players = df_players[['player_id', 'player_name']]
            df_players['team_id'] = None
            df_players['position'] = None
            df_players['jersey_number'] = None
            df_players['height'] = None
            df_players['weight'] = None
            
            # Load to database
            self.db.insert_dataframe(df_players, TABLES['players'], if_exists='replace')
            
            logger.info(f"Loaded {len(df_players)} players into database")
            return df_players
            
        except Exception as e:
            logger.error(f"Error extracting players: {e}")
            raise
    
    def extract_games_for_season(self, season, season_type='Regular Season'):
        """Extract all games for a specific season"""
        logger.info(f"Extracting games for season {season} - {season_type}...")
        
        try:
            # Fetch games
            gamefinder = leaguegamefinder.LeagueGameFinder(
                season_nullable=season,
                season_type_nullable=season_type
            )
            
            games_df = gamefinder.get_data_frames()[0]
            
            if games_df.empty:
                logger.warning(f"No games found for {season} - {season_type}")
                return pd.DataFrame()
            
            # Each game appears twice (once per team), deduplicate
            unique_games = games_df.drop_duplicates(subset=['GAME_ID'])
            
            logger.info(f"Found {len(unique_games)} unique games for {season}")
            return games_df  # Return full data for team stats extraction
            
        except Exception as e:
            logger.error(f"Error extracting games for {season}: {e}")
            return pd.DataFrame()
    
    def process_games_data(self, games_df, season):
        """Process games data and extract game and team stats"""
        if games_df.empty:
            return pd.DataFrame(), pd.DataFrame()
        
        # Process team stats (we have two rows per game, one for each team)
        team_stats = []
        for _, row in games_df.iterrows():
            team_stats.append({
                'game_id': row['GAME_ID'],
                'team_id': row['TEAM_ID'],
                'matchup': row['MATCHUP'],
                'wl': row['WL'],
                'pts': row['PTS'],
                'fgm': row['FGM'],
                'fga': row['FGA'],
                'fg_pct': row['FG_PCT'],
                'fg3m': row['FG3M'],
                'fg3a': row['FG3A'],
                'fg3_pct': row['FG3_PCT'],
                'ftm': row['FTM'],
                'fta': row['FTA'],
                'ft_pct': row['FT_PCT'],
                'oreb': row['OREB'],
                'dreb': row['DREB'],
                'reb': row['REB'],
                'ast': row['AST'],
                'stl': row['STL'],
                'blk': row['BLK'],
                'tov': row['TOV'],
                'pf': row['PF'],
                'plus_minus': row['PLUS_MINUS']
            })
        
        df_team_stats = pd.DataFrame(team_stats)
        
        # Process games table (deduplicate and extract game-level info)
        unique_games = games_df.drop_duplicates(subset=['GAME_ID'])
        
        games_list = []
        for _, row in unique_games.iterrows():
            # Determine home/away from matchup string
            matchup = row['MATCHUP']
            if ' @ ' in matchup:
                # Away game for this team
                is_home = False
            else:
                # Home game for this team
                is_home = True
            
            # Find both teams' scores for this game
            game_rows = games_df[games_df['GAME_ID'] == row['GAME_ID']]
            if len(game_rows) == 2:
                team1 = game_rows.iloc[0]
                team2 = game_rows.iloc[1]
                
                # Determine home and away based on matchup
                if '@' in team1['MATCHUP']:
                    away_team_id = team1['TEAM_ID']
                    home_team_id = team2['TEAM_ID']
                    away_score = team1['PTS']
                    home_score = team2['PTS']
                else:
                    home_team_id = team1['TEAM_ID']
                    away_team_id = team2['TEAM_ID']
                    home_score = team1['PTS']
                    away_score = team2['PTS']
            else:
                # Fallback if we don't have both teams
                home_team_id = row['TEAM_ID']
                away_team_id = None
                home_score = row['PTS']
                away_score = None
            
            games_list.append({
                'game_id': row['GAME_ID'],
                'season': season,
                'season_type': row['SEASON_ID'],
                'game_date': pd.to_datetime(row['GAME_DATE']).date(),
                'home_team_id': home_team_id,
                'away_team_id': away_team_id,
                'home_team_score': home_score,
                'away_team_score': away_score,
                'game_status': 'Final'
            })
        
        df_games = pd.DataFrame(games_list)
        
        return df_games, df_team_stats
    
    def extract_game_box_score(self, game_id):
        """Extract detailed box score for a specific game"""
        logger.info(f"Extracting box score for game {game_id}...")
        
        try:
            box_score = boxscoretraditionalv2.BoxScoreTraditionalV2(game_id=game_id)
            player_stats = box_score.player_stats.get_data_frame()
            
            if player_stats.empty:
                logger.warning(f"No player stats found for game {game_id}")
                return pd.DataFrame()
            
            # Select and rename columns
            player_stats_clean = player_stats[[
                'GAME_ID', 'PLAYER_ID', 'TEAM_ID', 'PLAYER_NAME',
                'START_POSITION', 'COMMENT', 'MIN',
                'FGM', 'FGA', 'FG_PCT', 'FG3M', 'FG3A', 'FG3_PCT',
                'FTM', 'FTA', 'FT_PCT', 'OREB', 'DREB', 'REB',
                'AST', 'STL', 'BLK', 'TO', 'PF', 'PTS', 'PLUS_MINUS'
            ]].copy()
            
            # Rename columns to match our schema
            player_stats_clean.columns = [
                'game_id', 'player_id', 'team_id', 'player_name',
                'start_position', 'comment', 'min',
                'fgm', 'fga', 'fg_pct', 'fg3m', 'fg3a', 'fg3_pct',
                'ftm', 'fta', 'ft_pct', 'oreb', 'dreb', 'reb',
                'ast', 'stl', 'blk', 'tov', 'pf', 'pts', 'plus_minus'
            ]
            
            return player_stats_clean
            
        except Exception as e:
            logger.error(f"Error extracting box score for game {game_id}: {e}")
            return pd.DataFrame()
    
    def run_full_ingestion(self, start_year=None, end_year=None):
        """Run the full ingestion pipeline"""
        start_year = start_year or ETL_CONFIG['start_season']
        end_year = end_year or ETL_CONFIG['end_season']
        
        logger.info("=" * 80)
        logger.info("Starting NBA Data Ingestion Pipeline")
        logger.info("=" * 80)
        
        # Step 1: Create tables
        self.db.create_tables()
        
        # Step 2: Extract and load teams
        self.extract_teams()
        time.sleep(self.rate_limit)
        
        # Step 3: Extract and load players
        self.extract_players()
        time.sleep(self.rate_limit)
        
        # Step 4: Extract games and stats by season
        seasons = self.generate_season_strings(start_year, end_year)
        
        for i, season in enumerate(seasons):
            logger.info(f"\n{'='*80}")
            logger.info(f"Processing season {season} ({i+1}/{len(seasons)})")
            logger.info(f"{'='*80}")
            
            # Extract games for this season
            games_df = self.extract_games_for_season(season, SEASON_TYPES['regular'])
            
            if not games_df.empty:
                # Process and load games and team stats
                df_games, df_team_stats = self.process_games_data(games_df, season)
                
                if not df_games.empty:
                    self.db.insert_dataframe(df_games, TABLES['games'], if_exists='append')
                    logger.info(f"Loaded {len(df_games)} games")
                
                if not df_team_stats.empty:
                    self.db.insert_dataframe(df_team_stats, TABLES['team_stats'], if_exists='append')
                    logger.info(f"Loaded {len(df_team_stats)} team stats records")
                
                # Save raw data to file
                raw_file = f"{ETL_CONFIG['raw_dir']}/games_{season}.csv"
                games_df.to_csv(raw_file, index=False)
                logger.info(f"Saved raw data to {raw_file}")
            
            # Rate limiting
            if i < len(seasons) - 1:
                logger.info(f"Sleeping {self.rate_limit} seconds...")
                time.sleep(self.rate_limit)
        
        logger.info("\n" + "=" * 80)
        logger.info("Ingestion Pipeline Completed Successfully!")
        logger.info("=" * 80)
        
        # Print summary statistics
        self.print_summary()
    
    def print_summary(self):
        """Print summary statistics of the ingested data"""
        logger.info("\nDatabase Summary:")
        logger.info("-" * 80)
        
        for table_name in [TABLES['teams'], TABLES['games'], TABLES['team_stats'], TABLES['players']]:
            try:
                count = self.db.get_row_count(table_name)
                logger.info(f"{table_name}: {count:,} rows")
            except Exception as e:
                logger.warning(f"Could not get count for {table_name}: {e}")


def main():
    """Main entry point"""
    db = DatabaseManager()
    
    try:
        ingester = NBADataIngester(db)
        
        # Run full ingestion
        # You can customize the year range here
        ingester.run_full_ingestion(start_year=2006, end_year=2025)
        
    except Exception as e:
        logger.error(f"Ingestion failed: {e}", exc_info=True)
        raise
    finally:
        db.close()


if __name__ == "__main__":
    main()