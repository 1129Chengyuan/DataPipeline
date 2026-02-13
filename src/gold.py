"""
Gold Layer: Load Silver parquets into PostgreSQL fact/dimension tables.

Usage:
    python gold.py 2024-01-15          # Load one date's fact data
    python gold.py 2024-01-15 --dims   # Also refresh dimensions + team stats

Uses staging-table upserts (INSERT ... ON CONFLICT DO UPDATE) so the
script can be re-run safely without duplicating data.

FK enforcement is handled by the database — rows that violate foreign
key constraints are caught and logged, not silently filtered in Python.
"""

import os
import logging
import argparse
from datetime import datetime

import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text

from config import settings

logger = logging.getLogger(__name__)

engine = create_engine(settings.db_url)
SILVER = settings.silver_path


# ── Schema initializer ───────────────────────────────────────────────

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS teams (
    id INT PRIMARY KEY,
    abbreviation TEXT,
    team_name TEXT
);

CREATE TABLE IF NOT EXISTS games (
    game_id TEXT PRIMARY KEY,
    season_id INT,
    game_date DATE
);

CREATE TABLE IF NOT EXISTS players (
    player_id INT PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    is_active BOOLEAN
);

CREATE TABLE IF NOT EXISTS fact_player_stats (
    game_id TEXT,
    player_id INT,
    team_id INT,
    start_position CHAR(3),
    comment TEXT,
    minutes NUMERIC(8,2),
    off_rating NUMERIC(8,3),
    def_rating NUMERIC(8,3),
    net_rating NUMERIC(8,3),
    usg_pct NUMERIC(8,3),
    ts_pct NUMERIC(8,3),
    efg_pct NUMERIC(8,3),
    pie NUMERIC(8,3),
    PRIMARY KEY (game_id, player_id),
    FOREIGN KEY (game_id) REFERENCES games(game_id),
    FOREIGN KEY (player_id) REFERENCES players(player_id),
    FOREIGN KEY (team_id) REFERENCES teams(id)
);

CREATE TABLE IF NOT EXISTS fact_team_stats (
    game_id TEXT,
    team_id INT,
    matchup TEXT,
    wl CHAR(1),
    minutes NUMERIC(8,2),
    pts INT,
    plus_minus INT,
    off_rating NUMERIC(8,3),
    def_rating NUMERIC(8,3),
    net_rating NUMERIC(8,3),
    pace NUMERIC(8,3),
    PRIMARY KEY (game_id, team_id),
    FOREIGN KEY (game_id) REFERENCES games(game_id),
    FOREIGN KEY (team_id) REFERENCES teams(id)
);

CREATE TABLE IF NOT EXISTS fact_play_by_play (
    game_id TEXT,
    action_number INT,
    period INT,
    clock_seconds NUMERIC(10,2),
    team_id INT,
    player_id INT,
    action_type TEXT,
    sub_type TEXT,
    description TEXT,
    shot_distance INT,
    score_home INT,
    score_away INT,
    is_field_goal BOOLEAN,
    PRIMARY KEY (game_id, action_number),
    FOREIGN KEY (game_id) REFERENCES games(game_id)
);
CREATE INDEX IF NOT EXISTS idx_pbp_game_clock
    ON fact_play_by_play(game_id, period, clock_seconds);

CREATE TABLE IF NOT EXISTS fact_shots (
    game_id TEXT,
    game_event_id INT,
    player_id INT,
    team_id INT,
    period INT,
    loc_x NUMERIC(10,2),
    loc_y NUMERIC(10,2),
    shot_distance INT,
    shot_attempted_flag BOOLEAN,
    shot_made_flag BOOLEAN,
    shot_zone_basic TEXT,
    shot_zone_area TEXT,
    shot_zone_range TEXT,
    PRIMARY KEY (game_id, game_event_id),
    FOREIGN KEY (game_id) REFERENCES games(game_id),
    FOREIGN KEY (player_id) REFERENCES players(player_id)
);
"""

def init_schema():
    """Ensure all tables exist. Safe to call on every run."""
    with engine.begin() as conn:
        conn.execute(text(SCHEMA_SQL))
    logger.info("Schema verified")


# ── Season helper ────────────────────────────────────────────────────

def get_nba_season(game_date: str) -> int:
    dt = datetime.strptime(game_date, "%Y-%m-%d")
    return dt.year if dt.month >= 10 else dt.year - 1


# ── Partitioned Silver reader ────────────────────────────────────────

def read_silver_partition(dataset: str, game_date: str) -> pd.DataFrame:
    season = get_nba_season(game_date)
    path = f"{SILVER}/{dataset}/season={season}/game_date={game_date}/data.parquet"
    if not os.path.exists(path):
        logger.warning("No Silver data at %s", path)
        return pd.DataFrame()
    return pd.read_parquet(path)


# ── Upsert helper ────────────────────────────────────────────────────

def upsert(df: pd.DataFrame, table_name: str, primary_keys: list[str]) -> int:
    """
    Idempotent load via a staging table.
    FK violations are caught by the database and logged — not silently
    skipped in Python.
    """
    if df.empty:
        logger.warning("Empty DataFrame, nothing to load into %s", table_name)
        return 0

    staging = f"_staging_{table_name}"

    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {staging}"))
        df.to_sql(staging, conn, if_exists="replace", index=False, method="multi")

        all_cols = list(df.columns)
        pk_clause = ", ".join(primary_keys)
        non_pk_cols = [c for c in all_cols if c not in primary_keys]

        insert_cols = ", ".join(all_cols)
        select_cols = ", ".join(f"s.{c}" for c in all_cols)

        if non_pk_cols:
            update_set = ", ".join(f"{c} = EXCLUDED.{c}" for c in non_pk_cols)
            conflict = f"ON CONFLICT ({pk_clause}) DO UPDATE SET {update_set}"
        else:
            conflict = f"ON CONFLICT ({pk_clause}) DO NOTHING"

        # Pre-join staging against referenced tables to avoid FK violations.
        # This is done in SQL (not Python) so it scales with database size.
        fk_joins = _build_fk_joins(table_name, staging)

        sql = f"""
            INSERT INTO {table_name} ({insert_cols})
            SELECT {select_cols} FROM {staging} s
            {fk_joins}
            {conflict}
        """
        result = conn.execute(text(sql))

        # Report any rows dropped by FK filtering
        staged = conn.execute(text(f"SELECT COUNT(*) FROM {staging}")).scalar()
        loaded = result.rowcount
        if loaded < staged:
            logger.warning(
                "%s: %d/%d rows loaded (%d skipped — missing FK refs)",
                table_name, loaded, staged, staged - loaded,
            )

        conn.execute(text(f"DROP TABLE IF EXISTS {staging}"))

    return loaded


def _build_fk_joins(table_name: str, staging: str) -> str:
    """
    Build SQL JOINs against dimension tables to pre-filter FK violations.
    This is the scalable replacement for loading entire dimension tables
    into Python sets — the filtering happens in the database via JOINs.
    """
    fk_map = {
        "fact_player_stats": [
            ("games", "s.game_id = g.game_id", "g"),
            ("players", "s.player_id = p.player_id", "p"),
            ("teams", "s.team_id = t.id", "t"),
        ],
        "fact_team_stats": [
            ("games", "s.game_id = g.game_id", "g"),
            ("teams", "s.team_id = t.id", "t"),
        ],
        "fact_play_by_play": [
            ("games", "s.game_id = g.game_id", "g"),
        ],
        "fact_shots": [
            ("games", "s.game_id = g.game_id", "g"),
            ("players", "s.player_id = p.player_id", "p"),
        ],
    }

    joins = fk_map.get(table_name, [])
    return " ".join(
        f"JOIN {dim} {alias} ON {cond}" for dim, cond, alias in joins
    )


# ── Dimension loaders (unpartitioned) ────────────────────────────────

def load_dim_players():
    logger.info("Loading dimension: players")
    df = pd.read_parquet(f"{SILVER}/players/players.parquet")
    df = df.rename(columns={"id": "player_id"})
    df = df[["player_id", "first_name", "last_name", "is_active"]]
    df = df.drop_duplicates(subset=["player_id"])
    count = upsert(df, "players", ["player_id"])
    logger.info("players: %d rows upserted", count)


def load_dim_teams():
    logger.info("Loading dimension: teams")
    df = pd.read_parquet(f"{SILVER}/teams/teams.parquet")
    teams_df = (
        df[["TEAM_ID", "TEAM_ABBREVIATION", "TEAM_NAME"]]
        .drop_duplicates(subset=["TEAM_ID"])
        .rename(columns={
            "TEAM_ID": "id",
            "TEAM_ABBREVIATION": "abbreviation",
            "TEAM_NAME": "team_name",
        })
    )
    count = upsert(teams_df, "teams", ["id"])
    logger.info("teams: %d rows upserted", count)


def load_dim_games():
    logger.info("Loading dimension: games")
    df = pd.read_parquet(f"{SILVER}/teams/teams.parquet")
    games_df = (
        df[["GAME_ID", "SEASON_ID", "GAME_DATE"]]
        .drop_duplicates(subset=["GAME_ID"])
        .rename(columns={
            "GAME_ID": "game_id",
            "SEASON_ID": "season_id",
            "GAME_DATE": "game_date",
        })
    )
    games_df["season_id"] = (
        games_df["season_id"].astype(str).str[-4:].astype(int)
    )
    count = upsert(games_df, "games", ["game_id"])
    logger.info("games: %d rows upserted", count)


# ── Fact loaders (date-partitioned) ──────────────────────────────────

def _parse_minutes_str(m):
    """Convert '12:34' or 'PT12M34.00S' to decimal minutes."""
    if pd.isna(m) or m == "" or m is None:
        return None
    s = str(m)
    try:
        if ":" in s and "PT" not in s:
            parts = s.split(":")
            return round(float(parts[0]) + float(parts[1]) / 60, 2)
        if s.startswith("PT"):
            s = s[2:]
            mins, rest = s.split("M")
            return round(float(mins) + float(rest.rstrip("S")) / 60, 2)
        return round(float(s), 2)
    except (ValueError, AttributeError):
        return None


def load_fact_player_stats(game_date: str):
    logger.info("Loading fact: player_stats (%s)", game_date)
    df = read_silver_partition("boxscores", game_date)
    if df.empty:
        return

    gold = df.rename(columns={
        "GAME_ID": "game_id", "PERSON_ID": "player_id", "TEAM_ID": "team_id",
        "POSITION": "start_position", "COMMENT": "comment", "minutes": "minutes",
        "offensiveRating": "off_rating", "defensiveRating": "def_rating",
        "netRating": "net_rating", "usagePercentage": "usg_pct",
        "trueShootingPercentage": "ts_pct",
        "effectiveFieldGoalPercentage": "efg_pct", "PIE": "pie",
    })

    cols = ["game_id", "player_id", "team_id", "start_position", "comment",
            "minutes", "off_rating", "def_rating", "net_rating",
            "usg_pct", "ts_pct", "efg_pct", "pie"]
    gold = gold[cols]
    gold["minutes"] = gold["minutes"].apply(_parse_minutes_str)
    gold = gold.drop_duplicates(subset=["game_id", "player_id"])
    count = upsert(gold, "fact_player_stats", ["game_id", "player_id"])
    logger.info("fact_player_stats: %d rows upserted", count)


def load_fact_pbp(game_date: str):
    logger.info("Loading fact: play_by_play (%s)", game_date)
    df = read_silver_partition("pbp", game_date)
    if df.empty:
        return

    gold = df.rename(columns={
        "GAME_ID": "game_id", "actionNumber": "action_number",
        "period": "period", "clock_seconds": "clock_seconds",
        "teamId": "team_id", "personId": "player_id",
        "actionType": "action_type", "subType": "sub_type",
        "description": "description", "shotDistance": "shot_distance",
        "scoreHome": "score_home", "scoreAway": "score_away",
        "isFieldGoal": "is_field_goal",
    })

    cols = ["game_id", "action_number", "period", "clock_seconds",
            "team_id", "player_id", "action_type", "sub_type",
            "description", "shot_distance", "score_home", "score_away",
            "is_field_goal"]
    gold = gold[cols]
    gold["team_id"] = gold["team_id"].replace(0, None)
    gold["player_id"] = gold["player_id"].replace(0, None)
    gold = gold.drop_duplicates(subset=["game_id", "action_number"])
    count = upsert(gold, "fact_play_by_play", ["game_id", "action_number"])
    logger.info("fact_play_by_play: %d rows upserted", count)


def load_fact_shots(game_date: str):
    logger.info("Loading fact: shots (%s)", game_date)
    df = read_silver_partition("shot_chart", game_date)
    if df.empty:
        return

    gold = df.rename(columns={
        "GAME_ID": "game_id", "GAME_EVENT_ID": "game_event_id",
        "PLAYER_ID": "player_id", "TEAM_ID": "team_id", "PERIOD": "period",
        "LOC_X": "loc_x", "LOC_Y": "loc_y",
        "SHOT_DISTANCE": "shot_distance",
        "SHOT_ATTEMPTED_FLAG": "shot_attempted_flag",
        "SHOT_MADE_FLAG": "shot_made_flag",
        "SHOT_ZONE_BASIC": "shot_zone_basic",
        "SHOT_ZONE_AREA": "shot_zone_area",
        "SHOT_ZONE_RANGE": "shot_zone_range",
    })

    cols = ["game_id", "game_event_id", "player_id", "team_id", "period",
            "loc_x", "loc_y", "shot_distance",
            "shot_attempted_flag", "shot_made_flag",
            "shot_zone_basic", "shot_zone_area", "shot_zone_range"]
    gold = gold[cols]
    gold = gold.drop_duplicates(subset=["game_id", "game_event_id"])
    count = upsert(gold, "fact_shots", ["game_id", "game_event_id"])
    logger.info("fact_shots: %d rows upserted", count)


# ── Bulk fact loaders (from unpartitioned files) ─────────────────────

def load_fact_team_stats():
    logger.info("Loading fact: team_stats (all history)")
    df = pd.read_parquet(f"{SILVER}/teams/teams.parquet")

    gold = df.rename(columns={
        "GAME_ID": "game_id", "TEAM_ID": "team_id",
        "MATCHUP": "matchup", "WL": "wl", "MIN": "minutes",
        "PTS": "pts", "PLUS_MINUS": "plus_minus",
    })
    gold = gold[["game_id", "team_id", "matchup", "wl",
                  "minutes", "pts", "plus_minus"]]
    gold["pts"] = pd.to_numeric(gold["pts"], errors="coerce")
    gold["plus_minus"] = pd.to_numeric(gold["plus_minus"], errors="coerce")
    gold["minutes"] = pd.to_numeric(gold["minutes"], errors="coerce")
    gold = gold.drop_duplicates(subset=["game_id", "team_id"])

    count = upsert(gold, "fact_team_stats", ["game_id", "team_id"])
    logger.info("fact_team_stats: %d rows upserted", count)


# ── Orchestration ────────────────────────────────────────────────────

def load_date(game_date: str):
    load_fact_player_stats(game_date)
    load_fact_pbp(game_date)
    load_fact_shots(game_date)


# ── CLI ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    parser = argparse.ArgumentParser(
        description="Load Silver Parquet partitions → PostgreSQL for a date."
    )
    parser.add_argument("game_date", help="YYYY-MM-DD")
    parser.add_argument("--dims", action="store_true",
                        help="Also load dimensions + team_stats fact.")
    args = parser.parse_args()

    logger.info("=" * 50)
    logger.info("Gold Layer: %s → PostgreSQL", args.game_date)
    logger.info("=" * 50)

    init_schema()

    if args.dims:
        load_dim_teams()
        load_dim_players()
        load_dim_games()
        load_fact_team_stats()

    load_date(args.game_date)
    logger.info("Done!")
