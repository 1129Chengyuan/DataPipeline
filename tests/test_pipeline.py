#!/usr/bin/env python3
"""
Comprehensive multi-season pipeline test.
Runs ingestion → extraction → gold for dates scattered across NBA history.
Then verifies data in Postgres.
"""

import subprocess
import sys

# Dates spanning ~25 years of NBA history
TEST_DATES = [
    "2000-01-15",   # 1999-00 season (early 2000s)
    "2005-12-25",   # 2005-06 season (Christmas Day)
    "2012-03-14",   # 2011-12 season (lockout shortened)
    "2019-06-13",   # 2018-19 Finals (Raptors vs Warriors)
    "2024-11-01",   # 2024-25 season (current)
]


def run(cmd: str, label: str):
    """Run a command and check for errors."""
    print(f"\n{'─' * 60}")
    print(f"  {label}")
    print(f"{'─' * 60}")
    result = subprocess.run(cmd, shell=True, capture_output=False)
    if result.returncode != 0:
        print(f"  ✘ FAILED: {label}")
        return False
    return True


def main():
    print("=" * 60)
    print("  COMPREHENSIVE MULTI-SEASON PIPELINE TEST")
    print("=" * 60)

    # Step 1: Ingest all test dates (with --dims on the first one)
    for i, date in enumerate(TEST_DATES):
        dims_flag = "--dims" if i == 0 else ""
        ok = run(
            f"python -m nba_etl.bronze.ingestion {date} {dims_flag}",
            f"INGEST: {date} {'(+dims)' if dims_flag else ''}")
        if not ok:
            sys.exit(1)

    # Step 2: Extract all test dates (with --dims on the first one)
    for i, date in enumerate(TEST_DATES):
        dims_flag = "--dims" if i == 0 else ""
        ok = run(
            f"python -m nba_etl.silver.extraction {date} {dims_flag}",
            f"EXTRACT: {date} {'(+dims)' if dims_flag else ''}")
        if not ok:
            sys.exit(1)

    # Step 3: Load gold layer (with --dims on the first one)
    for i, date in enumerate(TEST_DATES):
        dims_flag = "--dims" if i == 0 else ""
        ok = run(
            f"python -m nba_etl.gold.loading {date} {dims_flag}",
            f"GOLD: {date} {'(+dims)' if dims_flag else ''}")
        if not ok:
            sys.exit(1)

    # Step 4: Verify
    print(f"\n{'=' * 60}")
    print("  VERIFICATION")
    print(f"{'=' * 60}")

    from sqlalchemy import create_engine, text
    import os
    engine = create_engine(
        f"postgresql://{os.getenv('DB_USER','root')}:{os.getenv('DB_PASS','root')}"
        f"@{os.getenv('DB_HOST','pgdatabase')}:5432/{os.getenv('DB_NAME','mydatabase')}"
    )

    with engine.connect() as conn:
        # Table counts
        tables = ['teams', 'players', 'games',
                  'fact_team_stats', 'fact_player_stats',
                  'fact_play_by_play', 'fact_shots']
        print(f"\n{'Table':<25} {'Rows':>10}")
        print("-" * 37)
        for t in tables:
            count = conn.execute(text(f"SELECT COUNT(*) FROM {t}")).scalar()
            print(f"{t:<25} {count:>10,}")

        # Per-date breakdown for fact tables
        print(f"\n{'Game Date':<15} {'Players':>10} {'PBP':>10} {'Shots':>10}")
        print("-" * 47)
        for date in TEST_DATES:
            gids = [r[0] for r in conn.execute(
                text("SELECT game_id FROM games WHERE game_date = :d"),
                {"d": date}
            ).fetchall()]

            if not gids:
                print(f"{date:<15} {'(no games in dim)':>30}")
                continue

            placeholders = ",".join(f"'{g}'" for g in gids)

            ps = conn.execute(text(
                f"SELECT COUNT(*) FROM fact_player_stats WHERE game_id IN ({placeholders})"
            )).scalar()
            pbp = conn.execute(text(
                f"SELECT COUNT(*) FROM fact_play_by_play WHERE game_id IN ({placeholders})"
            )).scalar()
            shots = conn.execute(text(
                f"SELECT COUNT(*) FROM fact_shots WHERE game_id IN ({placeholders})"
            )).scalar()

            print(f"{date:<15} {ps:>10,} {pbp:>10,} {shots:>10,}")

        # Sample data quality check
        print(f"\n--- Sample fact_player_stats ---")
        rows = conn.execute(text(
            "SELECT game_id, player_id, off_rating, ts_pct, minutes "
            "FROM fact_player_stats WHERE minutes > 0 ORDER BY RANDOM() LIMIT 5"
        )).fetchall()
        for r in rows:
            print(f"  game={r[0]} player={r[1]} ORtg={r[2]} TS%={r[3]} MIN={r[4]}")

        print(f"\n--- Sample fact_play_by_play ---")
        rows = conn.execute(text(
            "SELECT game_id, period, clock_seconds, action_type, description "
            "FROM fact_play_by_play WHERE description != '' ORDER BY RANDOM() LIMIT 5"
        )).fetchall()
        for r in rows:
            print(f"  game={r[0]} Q{r[1]} {r[2]}s {r[3]}: {r[4]}")

    print(f"\n{'=' * 60}")
    print("  TEST COMPLETE")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
