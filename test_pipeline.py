"""
Test script to verify ETL pipeline components
"""

import sys
import logging
from datetime import datetime

from config import DB_CONFIG, ETL_CONFIG, TABLES
from db_utils import DatabaseManager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def test_database_connection():
    """Test database connection"""
    logger.info("Testing database connection...")
    
    try:
        db = DatabaseManager()
        conn = db.get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT version();")
        version = cursor.fetchone()
        logger.info(f"✓ Database connection successful")
        logger.info(f"  PostgreSQL version: {version[0]}")
        cursor.close()
        db.close()
        return True
    except Exception as e:
        logger.error(f"✗ Database connection failed: {e}")
        return False


def test_table_creation():
    """Test table creation"""
    logger.info("Testing table creation...")
    
    try:
        db = DatabaseManager()
        db.create_tables()
        
        # Check if tables exist
        for table_name in [TABLES['teams'], TABLES['games'], TABLES['players']]:
            if db.table_exists(table_name):
                logger.info(f"✓ Table '{table_name}' exists")
            else:
                logger.error(f"✗ Table '{table_name}' does not exist")
                return False
        
        db.close()
        return True
    except Exception as e:
        logger.error(f"✗ Table creation failed: {e}")
        return False


def test_data_ingestion():
    """Test data ingestion for a single season"""
    logger.info("Testing data ingestion (single season)...")
    
    try:
        from ingest import NBADataIngester
        
        db = DatabaseManager()
        ingester = NBADataIngester(db)
        
        # Test extracting teams
        df_teams = ingester.extract_teams()
        logger.info(f"✓ Extracted {len(df_teams)} teams")
        
        # Test extracting one season
        games_df = ingester.extract_games_for_season('2023-24', 'Regular Season')
        logger.info(f"✓ Extracted {len(games_df)} game records for 2023-24")
        
        db.close()
        return True
    except Exception as e:
        logger.error(f"✗ Data ingestion failed: {e}")
        return False


def test_data_transformation():
    """Test data transformation"""
    logger.info("Testing data transformation...")
    
    try:
        from transform import NBADataTransformer
        
        db = DatabaseManager()
        
        # Check if we have data to transform
        game_count = db.get_row_count(TABLES['games'])
        
        if game_count == 0:
            logger.warning("⚠ No games in database, skipping transformation test")
            db.close()
            return True
        
        transformer = NBADataTransformer(db)
        
        # Test creating aggregates
        df_aggregates = transformer.create_team_season_aggregates()
        logger.info(f"✓ Created {len(df_aggregates)} season aggregate records")
        
        db.close()
        return True
    except Exception as e:
        logger.error(f"✗ Data transformation failed: {e}")
        return False


def run_all_tests():
    """Run all tests"""
    logger.info("=" * 80)
    logger.info("NBA ETL Pipeline - Test Suite")
    logger.info("=" * 80)
    logger.info("")
    
    tests = [
        ("Database Connection", test_database_connection),
        ("Table Creation", test_table_creation),
        ("Data Ingestion", test_data_ingestion),
        ("Data Transformation", test_data_transformation),
    ]
    
    results = []
    
    for test_name, test_func in tests:
        logger.info(f"\n{'='*80}")
        logger.info(f"Running: {test_name}")
        logger.info(f"{'='*80}")
        
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            logger.error(f"Test '{test_name}' raised exception: {e}")
            results.append((test_name, False))
    
    # Print summary
    logger.info("\n" + "=" * 80)
    logger.info("Test Summary")
    logger.info("=" * 80)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for test_name, result in results:
        status = "✓ PASSED" if result else "✗ FAILED"
        logger.info(f"{test_name:.<50} {status}")
    
    logger.info("=" * 80)
    logger.info(f"Results: {passed}/{total} tests passed")
    logger.info("=" * 80)
    
    return all(result for _, result in results)


if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)