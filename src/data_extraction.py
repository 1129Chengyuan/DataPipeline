"""
Silver Layer: Transform Bronze JSON → partitioned Silver Parquet.

Usage:
    python data_extraction.py 2024-01-15          # Process one date's games
    python data_extraction.py 2024-01-15 --dims   # Also refresh dimension tables

Output structure:
    silver/boxscores/season=2023/game_date=2024-01-15/data.parquet
    silver/pbp/season=2023/game_date=2024-01-15/data.parquet
    silver/shot_chart/season=2023/game_date=2024-01-15/data.parquet
    silver/players/players.parquet       (unpartitioned dimension)
    silver/teams/teams.parquet           (unpartitioned dimension)
"""

import os
import json
import glob
import logging
import argparse
from datetime import datetime

import pandas as pd

from config import settings

logger = logging.getLogger(__name__)

BRONZE = settings.bronze_path
SILVER = settings.silver_path


# ── Season helper ────────────────────────────────────────────────────

def get_nba_season(game_date: str) -> int:
    """Oct+ = current year, Jan-Sep = year - 1."""
    dt = datetime.strptime(game_date, "%Y-%m-%d")
    return dt.year if dt.month >= 10 else dt.year - 1


# ── Manifest reader ──────────────────────────────────────────────────

def load_manifest(game_date: str) -> list[str]:
    manifest_path = f"{BRONZE}/manifests/{game_date}.json"
    if not os.path.exists(manifest_path):
        raise FileNotFoundError(
            f"No manifest at {manifest_path}. Run ingestion.py {game_date} first."
        )
    with open(manifest_path, "r") as f:
        return json.load(f)["game_ids"]


# ── Helpers ──────────────────────────────────────────────────────────

def _game_id_from_file(file_path: str) -> str:
    return os.path.basename(file_path).replace(".json", "")


def _save_partitioned(df: pd.DataFrame, dataset: str, season: int, game_date: str):
    out_dir = f"{SILVER}/{dataset}/season={season}/game_date={game_date}"
    os.makedirs(out_dir, exist_ok=True)
    out_path = f"{out_dir}/data.parquet"
    df.to_parquet(out_path, index=False)
    logger.info("%s → %d rows", out_path, len(df))


# ── Converters ───────────────────────────────────────────────────────

def convert_boxscore(file_path: str) -> pd.DataFrame:
    game_id = _game_id_from_file(file_path)
    with open(file_path, "r") as f:
        data = json.load(f)

    try:
        bs = data["boxScoreAdvanced"]
    except (KeyError, TypeError):
        logger.warning("No boxScoreAdvanced in %s, skipping", file_path)
        return pd.DataFrame()

    all_players = []
    for team_type in ["homeTeam", "awayTeam"]:
        team_data = bs.get(team_type, {})
        team_id = team_data.get("teamId")
        team_tricode = team_data.get("teamTricode", "")

        for player in team_data.get("players", []):
            row = {
                "PERSON_ID": player.get("personId"),
                "FIRST_NAME": player.get("firstName", ""),
                "FAMILY_NAME": player.get("familyName", ""),
                "NAME_I": player.get("nameI", ""),
                "POSITION": player.get("position", ""),
                "COMMENT": player.get("comment", ""),
                "JERSEY_NUM": player.get("jerseyNum", ""),
                "TEAM_ID": team_id,
                "TEAM_TRICODE": team_tricode,
                "TEAM_TYPE": "HOME" if team_type == "homeTeam" else "AWAY",
            }
            for k, v in player.get("statistics", {}).items():
                row[k] = v
            all_players.append(row)

    if not all_players:
        return pd.DataFrame()

    df = pd.DataFrame(all_players)
    df["GAME_ID"] = game_id
    df["PERSON_ID"] = pd.to_numeric(df["PERSON_ID"], errors="coerce").astype("Int64")
    df["TEAM_ID"] = pd.to_numeric(df["TEAM_ID"], errors="coerce").astype("Int64")
    for col in [c for c in df.columns if c not in (
            "GAME_ID", "PERSON_ID", "FIRST_NAME", "FAMILY_NAME", "NAME_I",
            "POSITION", "COMMENT", "JERSEY_NUM", "TEAM_ID", "TEAM_TRICODE",
            "TEAM_TYPE", "minutes")]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df.drop_duplicates(subset=["GAME_ID", "PERSON_ID"])


def convert_pbp(file_path: str) -> pd.DataFrame:
    game_id = _game_id_from_file(file_path)
    with open(file_path, "r") as f:
        data = json.load(f)

    try:
        actions = data["game"]["actions"]
    except (KeyError, TypeError):
        logger.warning("No actions in %s, skipping", file_path)
        return pd.DataFrame()
    if not actions:
        return pd.DataFrame()

    df = pd.DataFrame(actions)
    df["GAME_ID"] = game_id
    df["period"] = pd.to_numeric(df["period"], errors="coerce").astype("Int64")
    df["teamId"] = pd.to_numeric(df["teamId"], errors="coerce").astype("Int64")
    df["personId"] = pd.to_numeric(df["personId"], errors="coerce").astype("Int64")
    df["shotDistance"] = pd.to_numeric(df["shotDistance"], errors="coerce")
    df["isFieldGoal"] = df["isFieldGoal"].astype(bool)
    df["scoreHome"] = pd.to_numeric(df["scoreHome"], errors="coerce").astype("Int64")
    df["scoreAway"] = pd.to_numeric(df["scoreAway"], errors="coerce").astype("Int64")

    def _parse_clock(clock_str):
        if not isinstance(clock_str, str) or not clock_str.startswith("PT"):
            return None
        try:
            t = clock_str[2:]
            mins, rest = t.split("M")
            return float(mins) * 60 + float(rest.rstrip("S"))
        except (ValueError, AttributeError):
            return None

    df["clock_seconds"] = df["clock"].apply(_parse_clock)
    return df.drop_duplicates(subset=["GAME_ID", "actionNumber"])


def convert_shotchart(file_path: str) -> pd.DataFrame:
    game_id = _game_id_from_file(file_path)
    with open(file_path, "r") as f:
        data = json.load(f)

    try:
        results = data["resultSets"][0]
    except (KeyError, IndexError, TypeError):
        logger.warning("No resultSets in %s, skipping", file_path)
        return pd.DataFrame()

    df = pd.DataFrame(results["rowSet"], columns=results["headers"])
    df["GAME_ID"] = game_id
    df["PLAYER_ID"] = pd.to_numeric(df["PLAYER_ID"], errors="coerce").astype("Int64")
    df["TEAM_ID"] = pd.to_numeric(df["TEAM_ID"], errors="coerce").astype("Int64")
    df["PERIOD"] = pd.to_numeric(df["PERIOD"], errors="coerce").astype("Int64")
    df["SHOT_DISTANCE"] = pd.to_numeric(df["SHOT_DISTANCE"], errors="coerce")
    df["LOC_X"] = pd.to_numeric(df["LOC_X"], errors="coerce")
    df["LOC_Y"] = pd.to_numeric(df["LOC_Y"], errors="coerce")
    df["SHOT_ATTEMPTED_FLAG"] = df["SHOT_ATTEMPTED_FLAG"].astype(bool)
    df["SHOT_MADE_FLAG"] = df["SHOT_MADE_FLAG"].astype(bool)
    df["GAME_DATE"] = pd.to_datetime(df["GAME_DATE"], format="%Y%m%d", errors="coerce")
    return df.drop_duplicates(subset=["GAME_ID", "GAME_EVENT_ID"])


# ── Dimension processors (unpartitioned) ─────────────────────────────

def process_players():
    logger.info("Processing players...")
    with open(f"{BRONZE}/players/players.json", "r") as f:
        data = json.load(f)
    df = pd.DataFrame(data)
    df["id"] = df["id"].astype(int)
    df["is_active"] = df["is_active"].astype(bool)
    for col in ["full_name", "first_name", "last_name"]:
        df[col] = df[col].astype(str).str.strip()
    df = df.drop_duplicates(subset=["id"])
    out = f"{SILVER}/players"
    os.makedirs(out, exist_ok=True)
    df.to_parquet(f"{out}/players.parquet", index=False)
    logger.info("players → %d rows", len(df))


def process_teams():
    logger.info("Processing teams...")
    team_files = glob.glob(f"{BRONZE}/teams/*.json")
    frames = []
    for tf in sorted(team_files):
        try:
            with open(tf, "r") as f:
                data = json.load(f)
            headers = data["resultSets"][0]["headers"]
            rows = data["resultSets"][0]["rowSet"]
            df = pd.DataFrame(rows, columns=headers)
            df["TEAM_ID"] = df["TEAM_ID"].astype(int)
            df["PTS"] = pd.to_numeric(df["PTS"], errors="coerce").astype("Int64")
            df["PLUS_MINUS"] = pd.to_numeric(df["PLUS_MINUS"], errors="coerce")
            for col in ["FG_PCT", "FG3_PCT", "FT_PCT"]:
                df[col] = pd.to_numeric(df[col], errors="coerce")
            for col in ["FGM", "FGA", "FG3M", "FG3A", "FTM", "FTA",
                        "OREB", "DREB", "REB", "AST", "STL", "BLK", "TOV", "PF"]:
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
            df["GAME_DATE"] = pd.to_datetime(df["GAME_DATE"], errors="coerce")
            frames.append(df.drop_duplicates(subset=["GAME_ID", "TEAM_ID"]))
        except Exception as e:
            logger.error("Error in %s: %s", tf, e)

    df = pd.concat(frames, ignore_index=True).drop_duplicates(subset=["GAME_ID", "TEAM_ID"])
    out = f"{SILVER}/teams"
    os.makedirs(out, exist_ok=True)
    df.to_parquet(f"{out}/teams.parquet", index=False)
    logger.info("teams → %d rows", len(df))


# ── Date-based fact processors ───────────────────────────────────────

def process_date(game_date: str):
    game_ids = load_manifest(game_date)
    season = get_nba_season(game_date)
    logger.info("Processing %d games for %s (season %d)", len(game_ids), game_date, season)

    for dataset, converter, subfolder in [
        ("boxscores", convert_boxscore, "boxscores"),
        ("pbp", convert_pbp, "pbp"),
        ("shot_chart", convert_shotchart, "shot_chart"),
    ]:
        frames = []
        for gid in game_ids:
            path = f"{BRONZE}/{subfolder}/{gid}.json"
            if os.path.exists(path):
                df = converter(path)
                if not df.empty:
                    frames.append(df)
        if frames:
            _save_partitioned(pd.concat(frames, ignore_index=True), dataset, season, game_date)
        else:
            logger.warning("No %s data for %s", dataset, game_date)


# ── CLI ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    parser = argparse.ArgumentParser(
        description="Transform Bronze JSON → partitioned Silver Parquet for a date."
    )
    parser.add_argument("game_date", help="YYYY-MM-DD")
    parser.add_argument("--dims", action="store_true",
                        help="Also process dimension tables (players + teams).")
    args = parser.parse_args()

    logger.info("=" * 50)
    logger.info("Bronze → Silver: %s", args.game_date)
    logger.info("=" * 50)

    if args.dims:
        process_players()
        process_teams()

    process_date(args.game_date)
    logger.info("Done — Silver parquets written.")