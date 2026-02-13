"""
Bronze Layer Ingestion: Download raw NBA data for a single date.

Usage:
    python ingestion.py 2024-01-15          # Ingest all games on that date
    python ingestion.py 2024-01-15 --dims   # Also refresh dimension tables (teams/players)

Designed for Airflow: each DAG run calls this with one date, enabling
parallel backfills across an entire season.
"""

import os
import sys
import json
import time
import argparse
from nba_api.stats.static import teams, players
from nba_api.stats.endpoints import (
    scoreboardv2,
    shotchartdetail,
    boxscoreadvancedv3,
    playbyplayv3,
)

BRONZE_PATH = os.getenv("BRONZE_PATH", "/app/datalake/bronze")
RATE_LIMIT = 0.600  # seconds between API calls

for folder in ["teams", "players", "pbp", "boxscores", "shot_chart", "manifests"]:
    os.makedirs(f"{BRONZE_PATH}/{folder}", exist_ok=True)


# ── Helpers ──────────────────────────────────────────────────────────

def _save_json(data: dict, path: str):
    """Write JSON to disk."""
    with open(path, "w") as f:
        json.dump(data, f)


def _already_exists(path: str) -> bool:
    """Check if a file already exists (skip re-downloads)."""
    return os.path.exists(path)


# ── Date-based game discovery ────────────────────────────────────────

def get_game_ids_for_date(game_date: str) -> list[str]:
    """
    Use ScoreboardV2 to find all game IDs played on a specific date.

    Args:
        game_date: Date string in 'YYYY-MM-DD' format.

    Returns:
        List of game_id strings, e.g. ['0022300555', '0022300556', ...]
    """
    print(f"Looking up games on {game_date}...")
    sb = scoreboardv2.ScoreboardV2(game_date=game_date)
    data = sb.get_dict()

    game_header = [
        rs for rs in data["resultSets"] if rs["name"] == "GameHeader"
    ][0]

    headers = game_header["headers"]
    gi_idx = headers.index("GAME_ID")
    game_ids = [row[gi_idx] for row in game_header["rowSet"]]

    print(f"  Found {len(game_ids)} games on {game_date}")
    return game_ids


# ── Per-game downloaders ─────────────────────────────────────────────

def download_boxscore(game_id: str):
    """Download advanced boxscore (V3) for a single game."""
    path = f"{BRONZE_PATH}/boxscores/{game_id}.json"
    if _already_exists(path):
        return
    try:
        data = boxscoreadvancedv3.BoxScoreAdvancedV3(game_id=game_id).get_dict()
        _save_json(data, path)
        print(f"  ✔ boxscore {game_id}")
        time.sleep(RATE_LIMIT)
    except Exception as e:
        print(f"  ✘ boxscore {game_id}: {e}")


def download_pbp(game_id: str):
    """Download play-by-play (V3) for a single game."""
    path = f"{BRONZE_PATH}/pbp/{game_id}.json"
    if _already_exists(path):
        return
    try:
        data = playbyplayv3.PlayByPlayV3(game_id=game_id).get_dict()
        _save_json(data, path)
        print(f"  ✔ pbp     {game_id}")
        time.sleep(RATE_LIMIT)
    except Exception as e:
        print(f"  ✘ pbp     {game_id}: {e}")


def download_shotchart(game_id: str):
    """Download shot chart for a single game."""
    path = f"{BRONZE_PATH}/shot_chart/{game_id}.json"
    if _already_exists(path):
        return
    try:
        data = shotchartdetail.ShotChartDetail(
            team_id=0,
            player_id=0,
            game_id_nullable=game_id,
            context_measure_simple="FGA",
        ).get_dict()
        _save_json(data, path)
        print(f"  ✔ shots   {game_id}")
        time.sleep(RATE_LIMIT)
    except Exception as e:
        print(f"  ✘ shots   {game_id}: {e}")


# ── Dimension downloaders (run once / infrequently) ──────────────────

def download_all_teams():
    """Download full game history for all 30 teams."""
    from nba_api.stats.endpoints import leaguegamefinder

    print("Downloading team histories...")
    for team in teams.get_teams():
        team_id = team["id"]
        path = f"{BRONZE_PATH}/teams/{team_id}.json"
        if _already_exists(path):
            continue
        try:
            data = leaguegamefinder.LeagueGameFinder(
                team_id_nullable=team_id
            ).get_dict()
            _save_json(data, path)
            print(f"  ✔ {team['full_name']}")
            time.sleep(RATE_LIMIT)
        except Exception as e:
            print(f"  ✘ {team['full_name']}: {e}")


def download_all_players():
    """Download static player list."""
    path = f"{BRONZE_PATH}/players/players.json"
    try:
        _save_json(players.get_players(), path)
        print("  ✔ players")
    except Exception as e:
        print(f"  ✘ players: {e}")


# ── Orchestration ────────────────────────────────────────────────────

def ingest_date(game_date: str):
    """
    Full ingestion for one date:
      1. Look up game IDs via ScoreboardV2
      2. Download boxscores, PBP, and shot charts for each game
    """
    game_ids = get_game_ids_for_date(game_date)

    if not game_ids:
        print(f"  No games on {game_date}, nothing to do.")
        return

    print(f"Downloading data for {len(game_ids)} games...")
    for gid in game_ids:
        download_boxscore(gid)
        download_pbp(gid)
        download_shotchart(gid)

    # Save a manifest so downstream scripts know which games belong to this date
    manifest = {"game_date": game_date, "game_ids": game_ids}
    _save_json(manifest, f"{BRONZE_PATH}/manifests/{game_date}.json")

    print(f"✔ Done ingesting {game_date} ({len(game_ids)} games)")


# ── CLI ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Ingest NBA data for a specific date into Bronze layer."
    )
    parser.add_argument(
        "game_date",
        help="Date to ingest in YYYY-MM-DD format (e.g. 2024-01-15)",
    )
    parser.add_argument(
        "--dims",
        action="store_true",
        help="Also download dimension tables (teams + players). "
             "Only needed on first run or periodically.",
    )
    args = parser.parse_args()

    print("=" * 50)
    print(f"Bronze Ingestion: {args.game_date}")
    print("=" * 50)

    if args.dims:
        download_all_teams()
        download_all_players()

    ingest_date(args.game_date)
