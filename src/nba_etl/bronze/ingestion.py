"""
Bronze Layer Ingestion: Download raw NBA data for a single date.

Usage:
    python ingestion.py 2024-01-15          # Ingest all games on that date
    python ingestion.py 2024-01-15 --dims   # Also refresh dimension tables (teams/players)

Designed for Airflow: each DAG run calls this with one date, enabling
parallel backfills across an entire season.
"""

import os
import json
import time
import logging
import argparse

from nba_api.stats.static import teams, players
from nba_api.stats.endpoints import (
    scoreboardv2,
    shotchartdetail,
    boxscoreadvancedv3,
    playbyplayv3,
)
from nba_etl.config import settings

logger = logging.getLogger(__name__)

BRONZE = settings.bronze_path

for folder in ["teams", "players", "pbp", "boxscores", "shot_chart", "manifests"]:
    os.makedirs(f"{BRONZE}/{folder}", exist_ok=True)


# ── Helpers ──────────────────────────────────────────────────────────

def _save_json(data: dict, path: str):
    with open(path, "w") as f:
        json.dump(data, f)


def _already_exists(path: str) -> bool:
    return os.path.exists(path)


# ── Date-based game discovery ────────────────────────────────────────

def get_game_ids_for_date(game_date: str) -> list[str]:
    """Use ScoreboardV2 to find all game IDs played on a date."""
    logger.info("Looking up games on %s", game_date)
    sb = scoreboardv2.ScoreboardV2(game_date=game_date)
    data = sb.get_dict()

    game_header = [
        rs for rs in data["resultSets"] if rs["name"] == "GameHeader"
    ][0]

    headers = game_header["headers"]
    gi_idx = headers.index("GAME_ID")
    game_ids = [row[gi_idx] for row in game_header["rowSet"]]

    logger.info("Found %d games on %s", len(game_ids), game_date)
    return game_ids


# ── Per-game downloaders ─────────────────────────────────────────────

def download_boxscore(game_id: str):
    path = f"{BRONZE}/boxscores/{game_id}.json"
    if _already_exists(path):
        return
    try:
        data = boxscoreadvancedv3.BoxScoreAdvancedV3(game_id=game_id).get_dict()
        _save_json(data, path)
        logger.info("boxscore %s ✔", game_id)
        time.sleep(settings.rate_limit)
    except Exception as e:
        logger.error("boxscore %s ✘: %s", game_id, e)


def download_pbp(game_id: str):
    path = f"{BRONZE}/pbp/{game_id}.json"
    if _already_exists(path):
        return
    try:
        data = playbyplayv3.PlayByPlayV3(game_id=game_id).get_dict()
        _save_json(data, path)
        logger.info("pbp     %s ✔", game_id)
        time.sleep(settings.rate_limit)
    except Exception as e:
        logger.error("pbp     %s ✘: %s", game_id, e)


def download_shotchart(game_id: str):
    path = f"{BRONZE}/shot_chart/{game_id}.json"
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
        logger.info("shots   %s ✔", game_id)
        time.sleep(settings.rate_limit)
    except Exception as e:
        logger.error("shots   %s ✘: %s", game_id, e)


# ── Dimension downloaders (run once / infrequently) ──────────────────

def download_all_teams():
    from nba_api.stats.endpoints import leaguegamefinder

    logger.info("Downloading team histories...")
    for team in teams.get_teams():
        team_id = team["id"]
        path = f"{BRONZE}/teams/{team_id}.json"
        if _already_exists(path):
            continue
        try:
            data = leaguegamefinder.LeagueGameFinder(
                team_id_nullable=team_id
            ).get_dict()
            _save_json(data, path)
            logger.info("%s ✔", team["full_name"])
            time.sleep(settings.rate_limit)
        except Exception as e:
            logger.error("%s ✘: %s", team["full_name"], e)


def download_all_players():
    path = f"{BRONZE}/players/players.json"
    try:
        _save_json(players.get_players(), path)
        logger.info("players ✔")
    except Exception as e:
        logger.error("players ✘: %s", e)


# ── Orchestration ────────────────────────────────────────────────────

def ingest_date(game_date: str):
    """Full ingestion for one date."""
    game_ids = get_game_ids_for_date(game_date)

    if not game_ids:
        logger.info("No games on %s — writing empty manifest.", game_date)
        manifest = {"game_date": game_date, "game_ids": []}
        _save_json(manifest, f"{BRONZE}/manifests/{game_date}.json")
        return

    logger.info("Downloading data for %d games...", len(game_ids))
    for gid in game_ids:
        download_boxscore(gid)
        download_pbp(gid)
        download_shotchart(gid)

    # Save manifest for downstream scripts
    manifest = {"game_date": game_date, "game_ids": game_ids}
    _save_json(manifest, f"{BRONZE}/manifests/{game_date}.json")

    logger.info("Done ingesting %s (%d games)", game_date, len(game_ids))


# ── CLI ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

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
        help="Also download dimension tables (teams + players).",
    )
    args = parser.parse_args()

    logger.info("=" * 50)
    logger.info("Bronze Ingestion: %s", args.game_date)
    logger.info("=" * 50)

    if args.dims:
        download_all_teams()
        download_all_players()

    ingest_date(args.game_date)
