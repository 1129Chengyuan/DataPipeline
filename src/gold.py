"""
Gold Layer: Load Silver parquets into PostgreSQL fact/dimension tables.

Uses staging-table upserts (INSERT ... ON CONFLICT DO UPDATE) so the
script can be re-run safely without duplicating data.
"""

import os
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text

# ── Database connection ──────────────────────────────────────────────

db_user = os.getenv("DB_USER", "root")
db_pass = os.getenv("DB_PASS", "root")
db_name = os.getenv("DB_NAME", "mydatabase")
db_host = os.getenv("DB_HOST", "pgdatabase")
db_port = os.getenv("DB_PORT", "5432")

engine = create_engine(
    f"postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"
)

SILVER_PATH = "/app/datalake/silver"


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
    print("✔ Schema verified")

# ── Upsert helper ────────────────────────────────────────────────────

def upsert(df: pd.DataFrame, table_name: str, primary_keys: list[str]):
    """
    Idempotent load via a staging table:
      1. Write df to a temp staging table
      2. INSERT INTO target ... ON CONFLICT (pk) DO UPDATE SET ...
      3. Drop the staging table
    """
    staging = f"_staging_{table_name}"

    with engine.begin() as conn:
        # Drop leftover staging table
        conn.execute(text(f"DROP TABLE IF EXISTS {staging}"))

        # Write to staging
        df.to_sql(staging, conn, if_exists="replace", index=False, method="multi")

        # Build column lists
        all_cols = [c for c in df.columns]
        pk_clause = ", ".join(primary_keys)
        non_pk_cols = [c for c in all_cols if c not in primary_keys]

        insert_cols = ", ".join(all_cols)
        select_cols = ", ".join(all_cols)

        if non_pk_cols:
            update_set = ", ".join(
                f"{c} = EXCLUDED.{c}" for c in non_pk_cols
            )
            conflict = f"ON CONFLICT ({pk_clause}) DO UPDATE SET {update_set}"
        else:
            conflict = f"ON CONFLICT ({pk_clause}) DO NOTHING"

        sql = f"""
            INSERT INTO {table_name} ({insert_cols})
            SELECT {select_cols} FROM {staging}
            {conflict}
        """
        result = conn.execute(text(sql))
        conn.execute(text(f"DROP TABLE IF EXISTS {staging}"))

    return result.rowcount


# ── Dimension loaders ────────────────────────────────────────────────

def load_dim_players():
    """Silver players → players dimension table."""
    print("Loading dimension: players...")
    df = pd.read_parquet(f"{SILVER_PATH}/players/players.parquet")

    df = df.rename(columns={"id": "player_id"})
    df = df[["player_id", "first_name", "last_name", "is_active"]]
    df = df.drop_duplicates(subset=["player_id"])

    count = upsert(df, "players", ["player_id"])
    print(f"  ✔ {count} rows upserted into players")


def load_dim_teams():
    """
    Silver team game logs → teams dimension table.
    Extracts unique (TEAM_ID, TEAM_ABBREVIATION, TEAM_NAME) tuples.
    """
    print("Loading dimension: teams...")
    df = pd.read_parquet(f"{SILVER_PATH}/teams/team_game_logs.parquet")

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
    print(f"  ✔ {count} rows upserted into teams")


def load_dim_games():
    """
    Silver team game logs → games dimension table.
    Extracts unique (GAME_ID, SEASON_ID, GAME_DATE) tuples.
    """
    print("Loading dimension: games...")
    df = pd.read_parquet(f"{SILVER_PATH}/teams/team_game_logs.parquet")

    games_df = (
        df[["GAME_ID", "SEASON_ID", "GAME_DATE"]]
        .drop_duplicates(subset=["GAME_ID"])
        .rename(columns={
            "GAME_ID": "game_id",
            "SEASON_ID": "season_id",
            "GAME_DATE": "game_date",
        })
    )
    # season_id is stored as string like "22024" → extract last 4 digits as int
    games_df["season_id"] = (
        games_df["season_id"]
        .astype(str)
        .str[-4:]
        .astype(int)
    )

    count = upsert(games_df, "games", ["game_id"])
    print(f"  ✔ {count} rows upserted into games")


# ── Fact loaders ─────────────────────────────────────────────────────

def load_fact_team_stats():
    """
    Silver team game logs → fact_team_stats.
    The Silver file already has traditional stats. We don't have per-team
    advanced stats yet, so off_rating/def_rating/net_rating/pace are NULL
    for now (they'd come from a team-level boxscore endpoint).
    """
    print("Loading fact: team_stats...")
    df = pd.read_parquet(f"{SILVER_PATH}/teams/team_game_logs.parquet")

    gold = df.rename(columns={
        "GAME_ID": "game_id",
        "TEAM_ID": "team_id",
        "MATCHUP": "matchup",
        "WL": "wl",
        "MIN": "minutes",
        "PTS": "pts",
        "PLUS_MINUS": "plus_minus",
    })

    gold = gold[["game_id", "team_id", "matchup", "wl",
                  "minutes", "pts", "plus_minus"]]

    # Cast numeric types — use round() to handle float values before int cast
    gold["pts"] = pd.to_numeric(gold["pts"], errors="coerce")
    gold["plus_minus"] = pd.to_numeric(gold["plus_minus"], errors="coerce")
    gold["minutes"] = pd.to_numeric(gold["minutes"], errors="coerce")

    gold = gold.drop_duplicates(subset=["game_id", "team_id"])

    count = upsert(gold, "fact_team_stats", ["game_id", "team_id"])
    print(f"  ✔ {count} rows upserted into fact_team_stats")


def _parse_minutes_str(m):
    """Convert '12:34' or 'PT12M34.00S' to decimal minutes like 12.57."""
    if pd.isna(m) or m == "" or m is None:
        return None
    s = str(m)
    try:
        # Handle "MM:SS" format
        if ":" in s and "PT" not in s:
            parts = s.split(":")
            return round(float(parts[0]) + float(parts[1]) / 60, 2)
        # Handle "PT12M34.00S" format
        if s.startswith("PT"):
            s = s[2:]  # strip PT
            mins, rest = s.split("M")
            secs = rest.rstrip("S")
            return round(float(mins) + float(secs) / 60, 2)
        # Try direct float
        return round(float(s), 2)
    except (ValueError, AttributeError):
        return None


def load_fact_player_stats():
    """Silver boxscores (V3 advanced) → fact_player_stats."""
    print("Loading fact: player_stats...")
    df = pd.read_parquet(f"{SILVER_PATH}/boxscores/boxscores.parquet")

    gold = df.rename(columns={
        "GAME_ID": "game_id",
        "PERSON_ID": "player_id",
        "TEAM_ID": "team_id",
        "POSITION": "start_position",
        "COMMENT": "comment",
        "minutes": "minutes",
        "offensiveRating": "off_rating",
        "defensiveRating": "def_rating",
        "netRating": "net_rating",
        "usagePercentage": "usg_pct",
        "trueShootingPercentage": "ts_pct",
        "effectiveFieldGoalPercentage": "efg_pct",
        "PIE": "pie",
    })

    cols = ["game_id", "player_id", "team_id", "start_position", "comment",
            "minutes", "off_rating", "def_rating", "net_rating",
            "usg_pct", "ts_pct", "efg_pct", "pie"]
    gold = gold[cols]

    # Parse minutes string → decimal
    gold["minutes"] = gold["minutes"].apply(_parse_minutes_str)

    gold = gold.drop_duplicates(subset=["game_id", "player_id"])

    # Filter to only players/games that exist in dimension tables
    # (avoids FK violations for old games without dimension entries)
    with engine.connect() as conn:
        existing_games = set(
            r[0] for r in conn.execute(text("SELECT game_id FROM games")).fetchall()
        )
        existing_players = set(
            r[0] for r in conn.execute(text("SELECT player_id FROM players")).fetchall()
        )

    before = len(gold)
    gold = gold[
        gold["game_id"].isin(existing_games) &
        gold["player_id"].isin(existing_players)
    ]
    skipped = before - len(gold)
    if skipped:
        print(f"  ⚠ Skipped {skipped} rows (missing FK refs in games/players)")

    if gold.empty:
        print("  ⚠ No rows to load after FK filter.")
        return

    count = upsert(gold, "fact_player_stats", ["game_id", "player_id"])
    print(f"  ✔ {count} rows upserted into fact_player_stats")


def load_fact_pbp():
    """Silver play-by-play → fact_play_by_play."""
    print("Loading fact: play_by_play...")
    df = pd.read_parquet(f"{SILVER_PATH}/pbp/pbp.parquet")

    gold = df.rename(columns={
        "GAME_ID": "game_id",
        "actionNumber": "action_number",
        "period": "period",
        "clock_seconds": "clock_seconds",
        "teamId": "team_id",
        "personId": "player_id",
        "actionType": "action_type",
        "subType": "sub_type",
        "description": "description",
        "shotDistance": "shot_distance",
        "scoreHome": "score_home",
        "scoreAway": "score_away",
        "isFieldGoal": "is_field_goal",
    })

    cols = ["game_id", "action_number", "period", "clock_seconds",
            "team_id", "player_id", "action_type", "sub_type",
            "description", "shot_distance", "score_home", "score_away",
            "is_field_goal"]
    gold = gold[cols]

    # Replace 0 team/player IDs with NULL (0 means "no team/player" in V3)
    gold["team_id"] = gold["team_id"].replace(0, None)
    gold["player_id"] = gold["player_id"].replace(0, None)

    gold = gold.drop_duplicates(subset=["game_id", "action_number"])

    # Filter to games that exist
    with engine.connect() as conn:
        existing_games = set(
            r[0] for r in conn.execute(text("SELECT game_id FROM games")).fetchall()
        )
    before = len(gold)
    gold = gold[gold["game_id"].isin(existing_games)]
    skipped = before - len(gold)
    if skipped:
        print(f"  ⚠ Skipped {skipped} rows (game_id not in games dimension)")

    if gold.empty:
        print("  ⚠ No rows to load.")
        return

    count = upsert(gold, "fact_play_by_play", ["game_id", "action_number"])
    print(f"  ✔ {count} rows upserted into fact_play_by_play")


def load_fact_shots():
    """Silver shot charts → fact_shots."""
    print("Loading fact: shots...")
    df = pd.read_parquet(f"{SILVER_PATH}/shot_chart/shot_charts.parquet")

    gold = df.rename(columns={
        "GAME_ID": "game_id",
        "GAME_EVENT_ID": "game_event_id",
        "PLAYER_ID": "player_id",
        "TEAM_ID": "team_id",
        "PERIOD": "period",
        "LOC_X": "loc_x",
        "LOC_Y": "loc_y",
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

    # Filter to existing games + players
    with engine.connect() as conn:
        existing_games = set(
            r[0] for r in conn.execute(text("SELECT game_id FROM games")).fetchall()
        )
        existing_players = set(
            r[0] for r in conn.execute(text("SELECT player_id FROM players")).fetchall()
        )

    before = len(gold)
    gold = gold[
        gold["game_id"].isin(existing_games) &
        gold["player_id"].isin(existing_players)
    ]
    skipped = before - len(gold)
    if skipped:
        print(f"  ⚠ Skipped {skipped} rows (missing FK refs)")

    if gold.empty:
        print("  ⚠ No rows to load.")
        return

    count = upsert(gold, "fact_shots", ["game_id", "game_event_id"])
    print(f"  ✔ {count} rows upserted into fact_shots")


# ── Main ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 50)
    print("Gold Layer: Silver → PostgreSQL")
    print("=" * 50)

    # Schema first
    init_schema()

    # Dimensions FIRST (facts reference them via FK)
    load_dim_teams()
    load_dim_players()
    load_dim_games()

    # Facts
    load_fact_team_stats()
    load_fact_player_stats()
    load_fact_pbp()
    load_fact_shots()

    print()
    print("=" * 50)
    print("Done! All tables loaded.")
    print("=" * 50)
