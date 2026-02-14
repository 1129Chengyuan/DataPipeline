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

Data Contracts:
    Each converter validates its DataFrame before returning:
    - PK uniqueness
    - Critical column nulls
    - Value ranges (periods, percentages, coordinates, etc.)
    - Row count minimums
    Violations are logged as warnings/errors. A summary is printed per run.
"""

import os
import json
import glob
import logging
import argparse
from datetime import datetime
from dataclasses import dataclass, field

import pandas as pd

from nba_etl.config import settings

logger = logging.getLogger(__name__)

BRONZE = settings.bronze_path
SILVER = settings.silver_path


# ── Validation framework ─────────────────────────────────────────────

@dataclass
class ValidationResult:
    """Tracks validation issues for a single DataFrame."""
    source: str
    issues: list[str] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return len(self.issues) == 0

    def check_pk_unique(self, df: pd.DataFrame, pk_cols: list[str]):
        dupes = df[pk_cols].duplicated().sum()
        if dupes > 0:
            self.issues.append(f"DUPLICATE PK: {dupes} rows on {pk_cols}")
            logger.error("  ✘ %s: %d duplicate PKs on %s", self.source, dupes, pk_cols)

    def check_not_null(self, df: pd.DataFrame, columns: list[str]):
        for col in columns:
            if col not in df.columns:
                self.issues.append(f"MISSING COLUMN: {col}")
                logger.error("  ✘ %s: expected column '%s' not found", self.source, col)
                continue
            nulls = df[col].isnull().sum()
            if nulls > 0:
                self.issues.append(f"NULL: {col} has {nulls}/{len(df)} nulls")
                logger.warning("  ⚠ %s: %s has %d/%d nulls (%.1f%%)",
                               self.source, col, nulls, len(df), nulls * 100 / len(df))

    def check_range(self, df: pd.DataFrame, col: str, min_val=None, max_val=None):
        if col not in df.columns:
            return
        series = pd.to_numeric(df[col], errors="coerce").dropna()
        if series.empty:
            return
        if min_val is not None and (series < min_val).any():
            bad = (series < min_val).sum()
            self.issues.append(f"RANGE: {col} has {bad} values below {min_val}")
            logger.warning("  ⚠ %s: %s has %d values below %s", self.source, col, bad, min_val)
        if max_val is not None and (series > max_val).any():
            bad = (series > max_val).sum()
            self.issues.append(f"RANGE: {col} has {bad} values above {max_val}")
            logger.warning("  ⚠ %s: %s has %d values above %s", self.source, col, bad, max_val)

    def check_min_rows(self, df: pd.DataFrame, min_rows: int):
        if len(df) < min_rows:
            self.issues.append(f"ROW COUNT: {len(df)} rows (expected ≥{min_rows})")
            logger.warning("  ⚠ %s: only %d rows (expected ≥%d)", self.source, len(df), min_rows)

    def log_summary(self):
        if self.passed:
            logger.info("  ✔ %s: all checks passed", self.source)
        else:
            logger.warning("  ⚠ %s: %d issue(s) found", self.source, len(self.issues))


# Global list to collect all validation results for end-of-run report
_all_validations: list[ValidationResult] = []


def _validate(source: str) -> ValidationResult:
    """Create and register a new validation result."""
    v = ValidationResult(source=source)
    _all_validations.append(v)
    return v


# ── Season helper ────────────────────────────────────────────────────

def get_nba_season(game_date: str) -> int:
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
    df = df.drop_duplicates(subset=["GAME_ID", "PERSON_ID"])

    # ── Validation ──
    v = _validate(f"boxscore/{game_id}")
    v.check_pk_unique(df, ["GAME_ID", "PERSON_ID"])
    v.check_not_null(df, ["GAME_ID", "PERSON_ID", "TEAM_ID"])
    v.check_min_rows(df, 10)  # expect at least 10 players per game
    v.check_range(df, "offensiveRating", min_val=0, max_val=300)
    v.check_range(df, "defensiveRating", min_val=0, max_val=300)
    v.check_range(df, "usagePercentage", min_val=0, max_val=1)
    v.check_range(df, "trueShootingPercentage", min_val=0, max_val=2)
    v.check_range(df, "PIE", min_val=-1, max_val=1)
    v.log_summary()

    return df


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
    df = df.drop_duplicates(subset=["GAME_ID", "actionNumber"])

    # ── Validation ──
    v = _validate(f"pbp/{game_id}")
    v.check_pk_unique(df, ["GAME_ID", "actionNumber"])
    v.check_not_null(df, ["GAME_ID", "actionNumber", "period", "actionType"])
    v.check_min_rows(df, 100)  # expect at least 100 actions per game
    v.check_range(df, "period", min_val=1, max_val=10)  # up to 4OT
    v.check_range(df, "clock_seconds", min_val=0, max_val=720)
    v.check_range(df, "shotDistance", min_val=0, max_val=94)  # court is 94 ft
    v.check_range(df, "scoreHome", min_val=0)
    v.check_range(df, "scoreAway", min_val=0)
    v.log_summary()

    return df


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
    if df.empty:
        return df

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
    df = df.drop_duplicates(subset=["GAME_ID", "GAME_EVENT_ID"])

    # ── Validation ──
    v = _validate(f"shots/{game_id}")
    v.check_pk_unique(df, ["GAME_ID", "GAME_EVENT_ID"])
    v.check_not_null(df, ["GAME_ID", "GAME_EVENT_ID", "PLAYER_ID", "TEAM_ID", "PERIOD"])
    v.check_range(df, "PERIOD", min_val=1, max_val=10)
    v.check_range(df, "LOC_X", min_val=-250, max_val=250)   # half-court coords
    v.check_range(df, "LOC_Y", min_val=-50, max_val=900)
    v.check_range(df, "SHOT_DISTANCE", min_val=0, max_val=94)
    v.log_summary()

    return df


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

    # ── Validation ──
    v = _validate("players")
    v.check_pk_unique(df, ["id"])
    v.check_not_null(df, ["id", "full_name", "first_name", "last_name"])
    v.check_min_rows(df, 4000)  # NBA has had >4k players historically
    v.log_summary()

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

    # ── Validation ──
    v = _validate("teams")
    v.check_pk_unique(df, ["GAME_ID", "TEAM_ID"])
    v.check_not_null(df, ["GAME_ID", "TEAM_ID", "GAME_DATE", "PTS"])
    v.check_min_rows(df, 50_000)  # 30 teams × ~80 games/yr × 20+ years
    v.check_range(df, "PTS", min_val=0, max_val=200)
    v.check_range(df, "FG_PCT", min_val=0, max_val=1)
    v.check_range(df, "FG3_PCT", min_val=0, max_val=1)
    v.check_range(df, "FT_PCT", min_val=0, max_val=1)
    v.log_summary()

    out = f"{SILVER}/teams"
    os.makedirs(out, exist_ok=True)
    df.to_parquet(f"{out}/teams.parquet", index=False)
    logger.info("teams → %d rows", len(df))


# ── Date-based fact processors ───────────────────────────────────────

def process_date(game_date: str):
    game_ids = load_manifest(game_date)

    if not game_ids:
        logger.info("No games on %s — nothing to extract.", game_date)
        return

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


def print_validation_report():
    """Print a summary of all validation results for this run."""
    if not _all_validations:
        return

    passed = sum(1 for v in _all_validations if v.passed)
    failed = sum(1 for v in _all_validations if not v.passed)

    logger.info("─" * 50)
    logger.info("DATA VALIDATION REPORT")
    logger.info("─" * 50)
    logger.info("  Checked: %d datasets", len(_all_validations))
    logger.info("  Passed:  %d", passed)
    if failed:
        logger.warning("  Failed:  %d", failed)
        for v in _all_validations:
            if not v.passed:
                for issue in v.issues:
                    logger.warning("    %s: %s", v.source, issue)
    else:
        logger.info("  Failed:  0")
    logger.info("─" * 50)


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

    print_validation_report()
    logger.info("Done — Silver parquets written.")