"""
NBA Data Transformation Script
Transforms raw NBA data into analytics-ready datasets
"""

import os
import logging
from datetime import datetime
import pandas as pd
import numpy as np
from sqlalchemy import text

from config import ETL_CONFIG, TABLES
from db_utils import DatabaseManager

# Setup logging
os.makedirs(ETL_CONFIG['logs_dir'], exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"{ETL_CONFIG['logs_dir']}/transform.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class NBADataTransformer:
    """Handles transformation of NBA data for analytics"""
    
    def __init__(self, db_manager):
        self.db = db_manager
        os.makedirs(ETL_CONFIG['processed_dir'], exist_ok=True)
    
    def create_team_season_aggregates(self):
        """Create aggregated team statistics by season"""
        logger.info("Creating team season aggregates...")
        
        query = f"""
        SELECT 
            g.season,
            ts.team_id,
            t.team_name,
            t.team_abbreviation,
            COUNT(DISTINCT g.game_id) as games_played,
            SUM(CASE WHEN ts.wl = 'W' THEN 1 ELSE 0 END) as wins,
            SUM(CASE WHEN ts.wl = 'L' THEN 1 ELSE 0 END) as losses,
            ROUND(AVG(ts.pts), 2) as avg_points,
            ROUND(AVG(ts.fgm), 2) as avg_fgm,
            ROUND(AVG(ts.fga), 2) as avg_fga,
            ROUND(AVG(ts.fg_pct), 3) as avg_fg_pct,
            ROUND(AVG(ts.fg3m), 2) as avg_fg3m,
            ROUND(AVG(ts.fg3a), 2) as avg_fg3a,
            ROUND(AVG(ts.fg3_pct), 3) as avg_fg3_pct,
            ROUND(AVG(ts.ftm), 2) as avg_ftm,
            ROUND(AVG(ts.fta), 2) as avg_fta,
            ROUND(AVG(ts.ft_pct), 3) as avg_ft_pct,
            ROUND(AVG(ts.reb), 2) as avg_reb,
            ROUND(AVG(ts.ast), 2) as avg_ast,
            ROUND(AVG(ts.stl), 2) as avg_stl,
            ROUND(AVG(ts.blk), 2) as avg_blk,
            ROUND(AVG(ts.tov), 2) as avg_tov,
            ROUND(AVG(ts.pf), 2) as avg_pf,
            ROUND(AVG(ts.plus_minus), 2) as avg_plus_minus
        FROM {TABLES['team_stats']} ts
        JOIN {TABLES['games']} g ON ts.game_id = g.game_id
        JOIN {TABLES['teams']} t ON ts.team_id = t.team_id
        GROUP BY g.season, ts.team_id, t.team_name, t.team_abbreviation
        ORDER BY g.season DESC, wins DESC
        """
        
        df = self.db.execute_query(query)
        
        # Calculate win percentage
        df['win_pct'] = (df['wins'] / df['games_played']).round(3)
        
        # Save to processed data
        output_file = f"{ETL_CONFIG['processed_dir']}/team_season_stats.csv"
        df.to_csv(output_file, index=False)
        logger.info(f"Team season aggregates saved to {output_file}")
        
        return df
    
    def create_team_offensive_efficiency(self):
        """Calculate offensive efficiency metrics for teams"""
        logger.info("Creating team offensive efficiency metrics...")
        
        query = f"""
        SELECT 
            g.season,
            ts.team_id,
            t.team_abbreviation,
            AVG(ts.pts) as avg_points,
            AVG(ts.fga) as avg_fga,
            AVG(ts.fg3a) as avg_fg3a,
            AVG(ts.fta) as avg_fta,
            AVG(ts.tov) as avg_tov,
            AVG(ts.oreb) as avg_oreb,
            -- Offensive Efficiency: Points per 100 possessions
            -- Possessions = FGA + 0.44 * FTA - OREB + TOV
            ROUND(
                AVG(ts.pts) / 
                NULLIF(AVG(ts.fga + 0.44 * ts.fta - ts.oreb + ts.tov), 0) * 100,
                2
            ) as offensive_rating,
            -- True Shooting Percentage: PTS / (2 * (FGA + 0.44 * FTA))
            ROUND(
                AVG(ts.pts) / 
                NULLIF(2 * AVG(ts.fga + 0.44 * ts.fta), 0),
                3
            ) as true_shooting_pct,
            -- Effective Field Goal Percentage: (FGM + 0.5 * 3PM) / FGA
            ROUND(
                AVG(ts.fgm + 0.5 * ts.fg3m) / 
                NULLIF(AVG(ts.fga), 0),
                3
            ) as effective_fg_pct,
            -- Three Point Attempt Rate: 3PA / FGA
            ROUND(
                AVG(ts.fg3a) / 
                NULLIF(AVG(ts.fga), 0),
                3
            ) as three_point_rate,
            -- Free Throw Rate: FTA / FGA
            ROUND(
                AVG(ts.fta) / 
                NULLIF(AVG(ts.fga), 0),
                3
            ) as free_throw_rate
        FROM {TABLES['team_stats']} ts
        JOIN {TABLES['games']} g ON ts.game_id = g.game_id
        JOIN {TABLES['teams']} t ON ts.team_id = t.team_id
        GROUP BY g.season, ts.team_id, t.team_abbreviation
        ORDER BY g.season DESC, offensive_rating DESC
        """
        
        df = self.db.execute_query(query)
        
        output_file = f"{ETL_CONFIG['processed_dir']}/team_offensive_efficiency.csv"
        df.to_csv(output_file, index=False)
        logger.info(f"Team offensive efficiency saved to {output_file}")
        
        return df
    
    def create_team_defensive_stats(self):
        """Calculate defensive statistics by comparing opponent stats"""
        logger.info("Creating team defensive statistics...")
        
        # This requires joining games with opponent stats
        query = f"""
        WITH game_matchups AS (
            SELECT 
                g.game_id,
                g.season,
                g.game_date,
                g.home_team_id,
                g.away_team_id,
                h.pts as home_pts,
                a.pts as away_pts,
                h.fgm as home_fgm,
                h.fga as home_fga,
                a.fgm as away_fgm,
                a.fga as away_fga,
                h.reb as home_reb,
                a.reb as away_reb,
                h.tov as home_tov,
                a.tov as away_tov
            FROM {TABLES['games']} g
            JOIN {TABLES['team_stats']} h ON g.game_id = h.game_id AND g.home_team_id = h.team_id
            JOIN {TABLES['team_stats']} a ON g.game_id = a.game_id AND g.away_team_id = a.team_id
        ),
        home_defense AS (
            SELECT 
                season,
                home_team_id as team_id,
                AVG(away_pts) as avg_opp_points,
                AVG(away_fgm) as avg_opp_fgm,
                AVG(away_fga) as avg_opp_fga,
                AVG(away_reb) as avg_opp_reb,
                AVG(home_tov) as avg_forced_tov
            FROM game_matchups
            GROUP BY season, home_team_id
        ),
        away_defense AS (
            SELECT 
                season,
                away_team_id as team_id,
                AVG(home_pts) as avg_opp_points,
                AVG(home_fgm) as avg_opp_fgm,
                AVG(home_fga) as avg_opp_fga,
                AVG(home_reb) as avg_opp_reb,
                AVG(away_tov) as avg_forced_tov
            FROM game_matchups
            GROUP BY season, away_team_id
        ),
        combined_defense AS (
            SELECT 
                season,
                team_id,
                AVG(avg_opp_points) as avg_opp_points,
                AVG(avg_opp_fgm) as avg_opp_fgm,
                AVG(avg_opp_fga) as avg_opp_fga,
                AVG(avg_opp_reb) as avg_opp_reb,
                AVG(avg_forced_tov) as avg_forced_tov
            FROM (
                SELECT * FROM home_defense
                UNION ALL
                SELECT * FROM away_defense
            ) all_games
            GROUP BY season, team_id
        )
        SELECT 
            cd.season,
            cd.team_id,
            t.team_abbreviation,
            t.team_name,
            ROUND(cd.avg_opp_points, 2) as avg_opp_points,
            ROUND(cd.avg_opp_fgm / NULLIF(cd.avg_opp_fga, 0), 3) as opp_fg_pct,
            ROUND(cd.avg_opp_reb, 2) as avg_opp_reb,
            ROUND(cd.avg_forced_tov, 2) as avg_forced_tov
        FROM combined_defense cd
        JOIN {TABLES['teams']} t ON cd.team_id = t.team_id
        ORDER BY cd.season DESC, cd.avg_opp_points ASC
        """
        
        df = self.db.execute_query(query)
        
        output_file = f"{ETL_CONFIG['processed_dir']}/team_defensive_stats.csv"
        df.to_csv(output_file, index=False)
        logger.info(f"Team defensive stats saved to {output_file}")
        
        return df
    
    def create_game_trends_analysis(self):
        """Analyze scoring trends over time"""
        logger.info("Creating game trends analysis...")
        
        query = f"""
        SELECT 
            g.season,
            EXTRACT(YEAR FROM g.game_date) as year,
            EXTRACT(MONTH FROM g.game_date) as month,
            COUNT(*) as games_count,
            ROUND(AVG(g.home_team_score + g.away_team_score), 2) as avg_total_points,
            ROUND(AVG(g.home_team_score), 2) as avg_home_score,
            ROUND(AVG(g.away_team_score), 2) as avg_away_score,
            ROUND(AVG(ABS(g.home_team_score - g.away_team_score)), 2) as avg_score_differential
        FROM {TABLES['games']} g
        WHERE g.home_team_score IS NOT NULL 
          AND g.away_team_score IS NOT NULL
        GROUP BY g.season, EXTRACT(YEAR FROM g.game_date), EXTRACT(MONTH FROM g.game_date)
        ORDER BY year, month
        """
        
        df = self.db.execute_query(query)
        
        output_file = f"{ETL_CONFIG['processed_dir']}/scoring_trends.csv"
        df.to_csv(output_file, index=False)
        logger.info(f"Scoring trends saved to {output_file}")
        
        return df
    
    def create_head_to_head_records(self):
        """Create head-to-head records between teams"""
        logger.info("Creating head-to-head records...")
        
        query = f"""
        WITH all_matchups AS (
            -- Home games
            SELECT 
                g.season,
                g.home_team_id as team1_id,
                g.away_team_id as team2_id,
                CASE 
                    WHEN g.home_team_score > g.away_team_score THEN 1 
                    ELSE 0 
                END as team1_win
            FROM {TABLES['games']} g
            WHERE g.home_team_score IS NOT NULL
            
            UNION ALL
            
            -- Away games (flip the teams)
            SELECT 
                g.season,
                g.away_team_id as team1_id,
                g.home_team_id as team2_id,
                CASE 
                    WHEN g.away_team_score > g.home_team_score THEN 1 
                    ELSE 0 
                END as team1_win
            FROM {TABLES['games']} g
            WHERE g.away_team_score IS NOT NULL
        )
        SELECT 
            am.season,
            t1.team_abbreviation as team1,
            t2.team_abbreviation as team2,
            COUNT(*) as total_games,
            SUM(am.team1_win) as team1_wins,
            COUNT(*) - SUM(am.team1_win) as team2_wins
        FROM all_matchups am
        JOIN {TABLES['teams']} t1 ON am.team1_id = t1.team_id
        JOIN {TABLES['teams']} t2 ON am.team2_id = t2.team_id
        WHERE am.team1_id < am.team2_id  -- Avoid duplicates
        GROUP BY am.season, t1.team_abbreviation, t2.team_abbreviation
        HAVING COUNT(*) > 0
        ORDER BY am.season DESC, total_games DESC
        """
        
        df = self.db.execute_query(query)
        
        output_file = f"{ETL_CONFIG['processed_dir']}/head_to_head_records.csv"
        df.to_csv(output_file, index=False)
        logger.info(f"Head-to-head records saved to {output_file}")
        
        return df
    
    def create_home_away_splits(self):
        """Calculate home vs away performance splits"""
        logger.info("Creating home/away splits...")
        
        query = f"""
        WITH home_stats AS (
            SELECT 
                g.season,
                ts.team_id,
                'Home' as location,
                COUNT(*) as games,
                SUM(CASE WHEN ts.wl = 'W' THEN 1 ELSE 0 END) as wins,
                AVG(ts.pts) as avg_pts,
                AVG(ts.fg_pct) as avg_fg_pct,
                AVG(ts.fg3_pct) as avg_fg3_pct
            FROM {TABLES['team_stats']} ts
            JOIN {TABLES['games']} g ON ts.game_id = g.game_id
            WHERE g.home_team_id = ts.team_id
            GROUP BY g.season, ts.team_id
        ),
        away_stats AS (
            SELECT 
                g.season,
                ts.team_id,
                'Away' as location,
                COUNT(*) as games,
                SUM(CASE WHEN ts.wl = 'W' THEN 1 ELSE 0 END) as wins,
                AVG(ts.pts) as avg_pts,
                AVG(ts.fg_pct) as avg_fg_pct,
                AVG(ts.fg3_pct) as avg_fg3_pct
            FROM {TABLES['team_stats']} ts
            JOIN {TABLES['games']} g ON ts.game_id = g.game_id
            WHERE g.away_team_id = ts.team_id
            GROUP BY g.season, ts.team_id
        ),
        combined AS (
            SELECT * FROM home_stats
            UNION ALL
            SELECT * FROM away_stats
        )
        SELECT 
            c.season,
            t.team_abbreviation,
            c.location,
            c.games,
            c.wins,
            ROUND(c.wins::decimal / c.games, 3) as win_pct,
            ROUND(c.avg_pts, 2) as avg_points,
            ROUND(c.avg_fg_pct, 3) as avg_fg_pct,
            ROUND(c.avg_fg3_pct, 3) as avg_fg3_pct
        FROM combined c
        JOIN {TABLES['teams']} t ON c.team_id = t.team_id
        ORDER BY c.season DESC, t.team_abbreviation, c.location
        """
        
        df = self.db.execute_query(query)
        
        output_file = f"{ETL_CONFIG['processed_dir']}/home_away_splits.csv"
        df.to_csv(output_file, index=False)
        logger.info(f"Home/away splits saved to {output_file}")
        
        return df
    
    def create_analytics_views(self):
        """Create database views for common analytics queries"""
        logger.info("Creating analytics views...")
        
        views = [
            # Team season summary view
            (
                'vw_team_season_summary',
                f"""
                CREATE OR REPLACE VIEW vw_team_season_summary AS
                SELECT 
                    g.season,
                    ts.team_id,
                    t.team_name,
                    t.team_abbreviation,
                    COUNT(DISTINCT g.game_id) as games_played,
                    SUM(CASE WHEN ts.wl = 'W' THEN 1 ELSE 0 END) as wins,
                    SUM(CASE WHEN ts.wl = 'L' THEN 1 ELSE 0 END) as losses,
                    ROUND(AVG(ts.pts), 2) as ppg,
                    ROUND(AVG(ts.reb), 2) as rpg,
                    ROUND(AVG(ts.ast), 2) as apg,
                    ROUND(AVG(ts.fg_pct), 3) as fg_pct,
                    ROUND(AVG(ts.fg3_pct), 3) as fg3_pct,
                    ROUND(AVG(ts.ft_pct), 3) as ft_pct
                FROM {TABLES['team_stats']} ts
                JOIN {TABLES['games']} g ON ts.game_id = g.game_id
                JOIN {TABLES['teams']} t ON ts.team_id = t.team_id
                GROUP BY g.season, ts.team_id, t.team_name, t.team_abbreviation
                """
            ),
            
            # Recent games view
            (
                'vw_recent_games',
                f"""
                CREATE OR REPLACE VIEW vw_recent_games AS
                SELECT 
                    g.game_id,
                    g.season,
                    g.game_date,
                    ht.team_abbreviation as home_team,
                    at.team_abbreviation as away_team,
                    g.home_team_score,
                    g.away_team_score,
                    CASE 
                        WHEN g.home_team_score > g.away_team_score THEN ht.team_abbreviation
                        ELSE at.team_abbreviation
                    END as winner
                FROM {TABLES['games']} g
                JOIN {TABLES['teams']} ht ON g.home_team_id = ht.team_id
                JOIN {TABLES['teams']} at ON g.away_team_id = at.team_id
                WHERE g.home_team_score IS NOT NULL
                ORDER BY g.game_date DESC
                """
            )
        ]
        
        engine = self.db.get_engine()
        with engine.connect() as conn:
            for view_name, view_sql in views:
                try:
                    conn.execute(text(view_sql))
                    conn.commit()
                    logger.info(f"Created view: {view_name}")
                except Exception as e:
                    logger.error(f"Error creating view {view_name}: {e}")
        
        logger.info("Analytics views created successfully")
    
    def run_full_transformation(self):
        """Run the full transformation pipeline"""
        logger.info("=" * 80)
        logger.info("Starting NBA Data Transformation Pipeline")
        logger.info("=" * 80)
        
        # Run all transformations
        self.create_team_season_aggregates()
        self.create_team_offensive_efficiency()
        self.create_team_defensive_stats()
        self.create_game_trends_analysis()
        self.create_head_to_head_records()
        self.create_home_away_splits()
        self.create_analytics_views()
        
        logger.info("\n" + "=" * 80)
        logger.info("Transformation Pipeline Completed Successfully!")
        logger.info("=" * 80)
        
        # Print summary
        self.print_summary()
    
    def print_summary(self):
        """Print summary of transformed data"""
        logger.info("\nTransformed Data Summary:")
        logger.info("-" * 80)
        
        processed_files = []
        for file in os.listdir(ETL_CONFIG['processed_dir']):
            if file.endswith('.csv'):
                filepath = os.path.join(ETL_CONFIG['processed_dir'], file)
                df = pd.read_csv(filepath)
                processed_files.append((file, len(df)))
        
        for filename, row_count in processed_files:
            logger.info(f"{filename}: {row_count:,} rows")


def main():
    """Main entry point"""
    db = DatabaseManager()
    
    try:
        transformer = NBADataTransformer(db)
        transformer.run_full_transformation()
        
    except Exception as e:
        logger.error(f"Transformation failed: {e}", exc_info=True)
        raise
    finally:
        db.close()


if __name__ == "__main__":
    main()