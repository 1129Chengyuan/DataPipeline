#!/usr/bin/env python3
"""
Standalone backfill script — bulk-load historical NBA data.

Bypasses Airflow scheduling overhead by calling pipeline functions directly.
Idempotent: skips dates that already have a manifest + Silver parquet.
Rate-limit safe: random delays between dates + exponential backoff on failures.

Usage (from Docker):
    # Full 20-year backfill
    docker compose run --rm etl python scripts/backfill.py \
        --start 2004-01-01 --end 2024-10-01

    # Resume after interruption (just re-run same command — idempotent)
    docker compose run --rm etl python scripts/backfill.py \
        --start 2004-01-01 --end 2024-10-01

    # Force re-process + load dims first
    docker compose run --rm etl python scripts/backfill.py \
        --start 2024-01-01 --end 2024-06-01 --force --dims
"""

import os
import sys
import json
import time
import random
import argparse
import logging
from datetime import datetime, timedelta

# Add src/ to path so nba_etl package is importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from nba_etl.config import settings
from nba_etl.bronze.ingestion import (
    ingest_date, download_all_teams, download_all_players,
)
from nba_etl.silver.extraction import (
    process_date, process_players, process_teams,
    print_validation_report, _all_validations,
)
from nba_etl.gold.loading import load_date, init_schema, load_dim_players, load_dim_teams

logger = logging.getLogger("backfill")

BRONZE = settings.bronze_path

# ── Rate-limit / retry settings ─────────────────────────────────────
MAX_RETRIES = 5                # retries per date before giving up
BASE_BACKOFF = 30              # seconds (doubles each retry: 30, 60, 120, 240, 480)
INTER_DATE_DELAY = (1.0, 2.5)  # random sleep range between dates (seconds)
COOLDOWN_AFTER_ERRORS = 120    # seconds to pause after 3+ consecutive failures


def date_range(start: str, end: str):
    """Yield YYYY-MM-DD strings from start to end (inclusive)."""
    current = datetime.strptime(start, "%Y-%m-%d")
    end_dt = datetime.strptime(end, "%Y-%m-%d")
    while current <= end_dt:
        yield current.strftime("%Y-%m-%d")
        current += timedelta(days=1)


def is_already_done(game_date: str) -> bool:
    """Check if a date has already been fully processed."""
    manifest = f"{BRONZE}/manifests/{game_date}.json"
    if not os.path.exists(manifest):
        return False
    with open(manifest) as f:
        data = json.load(f)
    if not data.get("game_ids"):
        return True  # No-game day
    from nba_etl.silver.extraction import get_nba_season
    season = get_nba_season(game_date)
    silver_path = f"{settings.silver_path}/boxscores/season={season}/game_date={game_date}/data.parquet"
    return os.path.exists(silver_path)


def process_single_date(game_date: str):
    """Bronze → Silver → Gold for one date. Raises on failure."""
    ingest_date(game_date)
    process_date(game_date)
    load_date(game_date)


def process_with_retry(game_date: str) -> bool:
    """Try a date up to MAX_RETRIES times with exponential backoff.

    Returns True on success, False if all retries exhausted.
    """
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            process_single_date(game_date)
            return True
        except KeyboardInterrupt:
            raise
        except Exception as e:
            if attempt == MAX_RETRIES:
                logger.error("GIVING UP on %s after %d attempts: %s",
                             game_date, MAX_RETRIES, e)
                return False
            wait = BASE_BACKOFF * (2 ** (attempt - 1)) + random.uniform(0, 5)
            logger.warning(
                "Attempt %d/%d failed for %s: %s — retrying in %.0fs",
                attempt, MAX_RETRIES, game_date, e, wait,
            )
            time.sleep(wait)
    return False


def load_dimensions():
    """Download and load all dimension tables (teams + players)."""
    logger.info("=" * 60)
    logger.info("LOADING DIMENSION TABLES")
    logger.info("=" * 60)

    # Bronze: download raw team/player data
    download_all_teams()
    download_all_players()

    # Silver: transform to parquet
    process_players()
    process_teams()

    # Gold: load into Postgres
    load_dim_players()
    load_dim_teams()

    logger.info("Dimensions loaded ✔")


def run_backfill(start: str, end: str, skip_existing: bool = True,
                 load_dims: bool = False):
    """Run Bronze → Silver → Gold for every date in the range."""
    dates = list(date_range(start, end))
    total = len(dates)
    skipped = 0
    processed = 0
    failed = 0
    failed_dates = []
    consecutive_errors = 0

    logger.info("=" * 60)
    logger.info("BACKFILL: %s → %s (%d days)", start, end, total)
    logger.info("=" * 60)

    # Ensure Gold schema exists
    init_schema()

    # Load dimensions first so FK refs resolve
    if load_dims:
        load_dimensions()

    for i, game_date in enumerate(dates, 1):
        # Skip already-processed dates
        if skip_existing and is_already_done(game_date):
            skipped += 1
            if skipped % 100 == 0:
                logger.info("[%d/%d] Skipped %d already-processed dates...",
                            i, total, skipped)
            continue

        logger.info("─" * 60)
        logger.info("[%d/%d] Processing %s", i, total, game_date)
        logger.info("─" * 60)

        success = process_with_retry(game_date)

        if success:
            processed += 1
            consecutive_errors = 0
        else:
            failed += 1
            failed_dates.append(game_date)
            consecutive_errors += 1

            # If we get 3+ consecutive failures, the API is likely blocking us.
            # Take a long cooldown break before continuing.
            if consecutive_errors >= 3:
                logger.warning(
                    "⚠ %d consecutive failures — API may be rate-limiting. "
                    "Cooling down for %ds...",
                    consecutive_errors, COOLDOWN_AFTER_ERRORS,
                )
                time.sleep(COOLDOWN_AFTER_ERRORS)

        # Random delay between dates to stay under rate limits
        delay = random.uniform(*INTER_DATE_DELAY)
        time.sleep(delay)

        # Progress update every 50 processed dates
        if processed > 0 and processed % 50 == 0:
            logger.info(
                "PROGRESS: %d processed, %d skipped, %d failed out of %d total",
                processed, skipped, failed, i,
            )

    # Final summary
    logger.info("=" * 60)
    logger.info("BACKFILL COMPLETE")
    logger.info("=" * 60)
    logger.info("  Total dates:  %d", total)
    logger.info("  Processed:    %d", processed)
    logger.info("  Skipped:      %d (already done)", skipped)
    logger.info("  Failed:       %d", failed)
    if failed_dates:
        logger.info("  Failed dates: %s", ", ".join(failed_dates[:30]))
        if len(failed_dates) > 30:
            logger.info("  ... and %d more", len(failed_dates) - 30)

    # Validation report
    print_validation_report()
    _all_validations.clear()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    parser = argparse.ArgumentParser(
        description="Bulk backfill NBA data — bypasses Airflow for speed."
    )
    parser.add_argument("--start", required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument(
        "--force", action="store_true",
        help="Re-process dates even if they already have data.",
    )
    parser.add_argument(
        "--dims", action="store_true",
        help="Load dimension tables (teams + players) before starting backfill.",
    )
    args = parser.parse_args()

    run_backfill(args.start, args.end,
                 skip_existing=not args.force,
                 load_dims=args.dims)
