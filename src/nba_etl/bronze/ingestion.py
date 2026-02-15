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
import random
import logging
import argparse
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

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


# ── Rate-limit aware API caller ──────────────────────────────────────

_MAX_RETRIES = 5
_BASE_BACKOFF = 10  # seconds — doubles each retry: 10, 20, 40, 80, 160
_CALL_DELAY = 1.5   # seconds between successful calls
_JITTER = 1.0       # random 0-1s added to each delay
_MAX_CONCURRENT = 3  # max simultaneous API requests (threads)

_api_semaphore = threading.Semaphore(_MAX_CONCURRENT)

def _api_call(fn, label: str, *args, **kwargs):
    """
    Call an NBA API function with retry + exponential backoff.
    Uses a global semaphore to limit concurrent requests.
    Detects rate-limiting (timeouts, connection resets) and waits.
    Returns the result or raises after MAX_RETRIES.
    """
    for attempt in range(1, _MAX_RETRIES + 1):
        try:
            with _api_semaphore:
                result = fn(*args, **kwargs)
            # Success — sleep to stay under rate limits
            time.sleep(_CALL_DELAY + random.uniform(0, _JITTER))
            return result
        except KeyboardInterrupt:
            raise
        except Exception as e:
            err_str = str(e).lower()
            is_rate_limit = any(k in err_str for k in [
                "timed out", "read timeout", "connection aborted",
                "remotedisconnected", "429", "too many requests",
            ])

            if attempt == _MAX_RETRIES:
                logger.error("%s: FAILED after %d attempts: %s",
                             label, _MAX_RETRIES, e)
                raise

            wait = _BASE_BACKOFF * (2 ** (attempt - 1)) + random.uniform(0, 5)

            if is_rate_limit:
                # Rate limited — wait longer
                wait = max(wait, 30 * attempt)
                logger.warning(
                    "%s: rate-limited (attempt %d/%d) — waiting %.0fs: %s",
                    label, attempt, _MAX_RETRIES, wait, e,
                )
            else:
                logger.warning(
                    "%s: error (attempt %d/%d) — retrying in %.0fs: %s",
                    label, attempt, _MAX_RETRIES, wait, e,
                )

            time.sleep(wait)

    # Should never reach here
    raise RuntimeError(f"{label}: exhausted all retries")


# ── Date-based game discovery ────────────────────────────────────────

def get_game_ids_for_date(game_date: str) -> list[str]:
    """Use ScoreboardV2 to find all game IDs played on a date."""
    logger.info("Looking up games on %s", game_date)
    sb = _api_call(
        scoreboardv2.ScoreboardV2,
        f"scoreboard/{game_date}",
        game_date=game_date,
    )
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
    result = _api_call(
        boxscoreadvancedv3.BoxScoreAdvancedV3,
        f"boxscore/{game_id}",
        game_id=game_id,
    )
    _save_json(result.get_dict(), path)
    logger.info("boxscore %s ✔", game_id)


def download_pbp(game_id: str):
    path = f"{BRONZE}/pbp/{game_id}.json"
    if _already_exists(path):
        return
    result = _api_call(
        playbyplayv3.PlayByPlayV3,
        f"pbp/{game_id}",
        game_id=game_id,
    )
    _save_json(result.get_dict(), path)
    logger.info("pbp     %s ✔", game_id)


def download_shotchart(game_id: str):
    path = f"{BRONZE}/shot_chart/{game_id}.json"
    if _already_exists(path):
        return
    result = _api_call(
        shotchartdetail.ShotChartDetail,
        f"shots/{game_id}",
        team_id=0,
        player_id=0,
        game_id_nullable=game_id,
        context_measure_simple="FGA",
    )
    _save_json(result.get_dict(), path)
    logger.info("shots   %s ✔", game_id)


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

def _download_game(game_id: str):
    """Download all 3 files for a single game (called from threads)."""
    download_boxscore(game_id)
    download_pbp(game_id)
    download_shotchart(game_id)


def ingest_date(game_date: str):
    """Full ingestion for one date, with threaded per-game downloads."""
    game_ids = get_game_ids_for_date(game_date)

    if not game_ids:
        logger.info("No games on %s — writing empty manifest.", game_date)
        manifest = {"game_date": game_date, "game_ids": []}
        _save_json(manifest, f"{BRONZE}/manifests/{game_date}.json")
        return

    logger.info("Downloading data for %d games (threaded)...", len(game_ids))

    # Download games concurrently — the _api_semaphore inside _api_call
    # limits actual simultaneous HTTP requests to _MAX_CONCURRENT
    errors = []
    with ThreadPoolExecutor(max_workers=_MAX_CONCURRENT) as pool:
        futures = {pool.submit(_download_game, gid): gid for gid in game_ids}
        for future in as_completed(futures):
            gid = futures[future]
            try:
                future.result()
            except Exception as e:
                logger.error("Game %s failed: %s", gid, e)
                errors.append(gid)

    if errors:
        raise RuntimeError(
            f"{len(errors)} game(s) failed on {game_date}: {errors}"
        )

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
