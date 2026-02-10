"""
NBA ETL Pipeline Orchestrator
Main script to run the complete ETL pipeline
"""

import argparse
import logging
import sys
from datetime import datetime

from config import ETL_CONFIG
from db_utils import DatabaseManager
from ingest import NBADataIngester
from transform import NBADataTransformer

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"{ETL_CONFIG['logs_dir']}/etl_pipeline.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def run_etl_pipeline(start_year=None, end_year=None, skip_ingest=False, skip_transform=False):
    """
    Run the complete ETL pipeline
    
    Args:
        start_year: Starting year for data ingestion
        end_year: Ending year for data ingestion
        skip_ingest: If True, skip the ingestion phase
        skip_transform: If True, skip the transformation phase
    """
    start_time = datetime.now()
    logger.info("=" * 100)
    logger.info(f"NBA ETL PIPELINE STARTED - {start_time}")
    logger.info("=" * 100)
    
    db = DatabaseManager()
    
    try:
        # Phase 1: Ingestion (Extract & Load)
        if not skip_ingest:
            logger.info("\n" + "=" * 100)
            logger.info("PHASE 1: DATA INGESTION (EXTRACT & LOAD)")
            logger.info("=" * 100)
            
            ingester = NBADataIngester(db)
            ingester.run_full_ingestion(start_year=start_year, end_year=end_year)
        else:
            logger.info("\nSkipping ingestion phase...")
        
        # Phase 2: Transformation
        if not skip_transform:
            logger.info("\n" + "=" * 100)
            logger.info("PHASE 2: DATA TRANSFORMATION")
            logger.info("=" * 100)
            
            transformer = NBADataTransformer(db)
            transformer.run_full_transformation()
        else:
            logger.info("\nSkipping transformation phase...")
        
        # Pipeline Complete
        end_time = datetime.now()
        duration = end_time - start_time
        
        logger.info("\n" + "=" * 100)
        logger.info(f"ETL PIPELINE COMPLETED SUCCESSFULLY!")
        logger.info(f"Total Duration: {duration}")
        logger.info(f"Started: {start_time}")
        logger.info(f"Ended: {end_time}")
        logger.info("=" * 100)
        
        return True
        
    except Exception as e:
        logger.error(f"\n{'='*100}")
        logger.error(f"ETL PIPELINE FAILED!")
        logger.error(f"Error: {e}")
        logger.error(f"{'='*100}", exc_info=True)
        return False
        
    finally:
        db.close()


def main():
    """Main entry point with CLI arguments"""
    parser = argparse.ArgumentParser(description='NBA ETL Pipeline')
    
    parser.add_argument(
        '--start-year',
        type=int,
        default=ETL_CONFIG['start_season'],
        help=f"Starting season year (default: {ETL_CONFIG['start_season']})"
    )
    
    parser.add_argument(
        '--end-year',
        type=int,
        default=ETL_CONFIG['end_season'],
        help=f"Ending season year (default: {ETL_CONFIG['end_season']})"
    )
    
    parser.add_argument(
        '--skip-ingest',
        action='store_true',
        help='Skip the ingestion phase (use existing data)'
    )
    
    parser.add_argument(
        '--skip-transform',
        action='store_true',
        help='Skip the transformation phase'
    )
    
    parser.add_argument(
        '--ingest-only',
        action='store_true',
        help='Run only the ingestion phase'
    )
    
    parser.add_argument(
        '--transform-only',
        action='store_true',
        help='Run only the transformation phase'
    )
    
    args = parser.parse_args()
    
    # Handle mutually exclusive options
    skip_ingest = args.skip_ingest or args.transform_only
    skip_transform = args.skip_transform or args.ingest_only
    
    # Run the pipeline
    success = run_etl_pipeline(
        start_year=args.start_year,
        end_year=args.end_year,
        skip_ingest=skip_ingest,
        skip_transform=skip_transform
    )
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()