"""
Silver Layer (PySpark): Transform Bronze JSON → partitioned Silver Parquet.

Spark-based replacement for extraction.py — same input/output contract,
using PySpark DataFrames, UDFs, window functions, and schema enforcement.

Usage:
    python -m nba_etl.silver.spark_extraction 2024-01-15
    python -m nba_etl.silver.spark_extraction 2024-01-15 --dims

Output structure (identical to pandas version):
    silver/boxscores/season=2023/game_date=2024-01-15/data.parquet
    silver/pbp/season=2023/game_date=2024-01-15/data.parquet
    silver/shot_chart/season=2023/game_date=2024-01-15/data.parquet
"""

import os
import json
import glob
import logging
import argparse
from datetime import datetime
from functools import reduce

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

from nba_etl.config import settings

logger = logging.getLogger(__name__)

BRONZE = settings.bronze_path
SILVER = settings.silver_path


# ── Spark session ────────────────────────────────────────────────────

_spark = None


def get_spark() -> SparkSession:
    """Lazy singleton SparkSession configured for local mode."""
    global _spark
    if _spark is None:
        _spark = (
            SparkSession.builder
            .master("local[*]")
            .appName("nba-etl-silver")
            .config("spark.sql.parquet.compression.codec", "snappy")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.driver.memory", "2g")
            .config("spark.ui.enabled", "false")       # no web UI overhead
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.ansi.enabled", "false")  # permissive casts ('' → null)
            .getOrCreate()
        )
        _spark.sparkContext.setLogLevel("WARN")
        logger.info("SparkSession created [local mode, %d cores]",
                     _spark.sparkContext.defaultParallelism)
    return _spark


# ── Season helper ────────────────────────────────────────────────────

def get_nba_season(game_date: str) -> int:
    dt = datetime.strptime(game_date, "%Y-%m-%d")
    return dt.year if dt.month >= 10 else dt.year - 1


# ── Manifest reader ─────────────────────────────────────────────────

def load_manifest(game_date: str) -> list[str]:
    manifest_path = f"{BRONZE}/manifests/{game_date}.json"
    if not os.path.exists(manifest_path):
        raise FileNotFoundError(
            f"No manifest at {manifest_path}. Run ingestion.py {game_date} first."
        )
    with open(manifest_path, "r") as f:
        return json.load(f)["game_ids"]


# ── Schemas ──────────────────────────────────────────────────────────

BOXSCORE_SCHEMA = T.StructType([
    T.StructField("GAME_ID", T.StringType(), False),
    T.StructField("PERSON_ID", T.LongType(), False),
    T.StructField("FIRST_NAME", T.StringType(), True),
    T.StructField("FAMILY_NAME", T.StringType(), True),
    T.StructField("NAME_I", T.StringType(), True),
    T.StructField("POSITION", T.StringType(), True),
    T.StructField("COMMENT", T.StringType(), True),
    T.StructField("JERSEY_NUM", T.StringType(), True),
    T.StructField("TEAM_ID", T.LongType(), False),
    T.StructField("TEAM_TRICODE", T.StringType(), True),
    T.StructField("TEAM_TYPE", T.StringType(), True),
    T.StructField("minutes", T.StringType(), True),
    T.StructField("offensiveRating", T.DoubleType(), True),
    T.StructField("defensiveRating", T.DoubleType(), True),
    T.StructField("netRating", T.DoubleType(), True),
    T.StructField("usagePercentage", T.DoubleType(), True),
    T.StructField("trueShootingPercentage", T.DoubleType(), True),
    T.StructField("effectiveFieldGoalPercentage", T.DoubleType(), True),
    T.StructField("PIE", T.DoubleType(), True),
])

PBP_SCHEMA = T.StructType([
    T.StructField("GAME_ID", T.StringType(), False),
    T.StructField("actionNumber", T.IntegerType(), False),
    T.StructField("period", T.IntegerType(), True),
    T.StructField("clock", T.StringType(), True),
    T.StructField("clock_seconds", T.DoubleType(), True),
    T.StructField("teamId", T.LongType(), True),
    T.StructField("personId", T.LongType(), True),
    T.StructField("actionType", T.StringType(), True),
    T.StructField("subType", T.StringType(), True),
    T.StructField("description", T.StringType(), True),
    T.StructField("shotDistance", T.DoubleType(), True),
    T.StructField("scoreHome", T.IntegerType(), True),
    T.StructField("scoreAway", T.IntegerType(), True),
    T.StructField("isFieldGoal", T.BooleanType(), True),
])

SHOTCHART_SCHEMA = T.StructType([
    T.StructField("GAME_ID", T.StringType(), False),
    T.StructField("GAME_EVENT_ID", T.IntegerType(), False),
    T.StructField("PLAYER_ID", T.LongType(), True),
    T.StructField("TEAM_ID", T.LongType(), True),
    T.StructField("PERIOD", T.IntegerType(), True),
    T.StructField("LOC_X", T.DoubleType(), True),
    T.StructField("LOC_Y", T.DoubleType(), True),
    T.StructField("SHOT_DISTANCE", T.IntegerType(), True),
    T.StructField("SHOT_ATTEMPTED_FLAG", T.BooleanType(), True),
    T.StructField("SHOT_MADE_FLAG", T.BooleanType(), True),
    T.StructField("SHOT_ZONE_BASIC", T.StringType(), True),
    T.StructField("SHOT_ZONE_AREA", T.StringType(), True),
    T.StructField("SHOT_ZONE_RANGE", T.StringType(), True),
])


# ── UDFs ─────────────────────────────────────────────────────────────

@F.udf(T.DoubleType())
def parse_clock_udf(clock_str):
    """Parse ISO 8601 game clock 'PT05M30.00S' → 330.0 seconds."""
    if not clock_str or not clock_str.startswith("PT"):
        return None
    try:
        t = clock_str[2:]
        mins, rest = t.split("M")
        return float(mins) * 60 + float(rest.rstrip("S"))
    except (ValueError, AttributeError):
        return None


# ── Converters ───────────────────────────────────────────────────────

def convert_boxscore_spark(file_path: str) -> DataFrame:
    """Transform a single Bronze boxscore JSON → Spark DataFrame."""
    spark = get_spark()
    game_id = os.path.basename(file_path).replace(".json", "")

    with open(file_path, "r") as f:
        data = json.load(f)

    try:
        bs = data["boxScoreAdvanced"]
    except (KeyError, TypeError):
        logger.warning("No boxScoreAdvanced in %s, skipping", file_path)
        return spark.createDataFrame([], BOXSCORE_SCHEMA)

    # Flatten nested home/away → player rows
    all_players = []
    for team_type in ["homeTeam", "awayTeam"]:
        team_data = bs.get(team_type, {})
        team_id = team_data.get("teamId")
        team_tricode = team_data.get("teamTricode", "")

        for player in team_data.get("players", []):
            stats = player.get("statistics", {})
            all_players.append({
                "GAME_ID": game_id,
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
                "minutes": stats.get("minutes", ""),
                "offensiveRating": stats.get("offensiveRating"),
                "defensiveRating": stats.get("defensiveRating"),
                "netRating": stats.get("netRating"),
                "usagePercentage": stats.get("usagePercentage"),
                "trueShootingPercentage": stats.get("trueShootingPercentage"),
                "effectiveFieldGoalPercentage": stats.get("effectiveFieldGoalPercentage"),
                "PIE": stats.get("PIE"),
            })

    if not all_players:
        return spark.createDataFrame([], BOXSCORE_SCHEMA)

    df = spark.createDataFrame(all_players)

    # ── Type casting ──
    # spark.sql.ansi.enabled=false makes cast() return null for malformed input
    df = (
        df
        .withColumn("PERSON_ID", F.col("PERSON_ID").cast(T.LongType()))
        .withColumn("TEAM_ID", F.col("TEAM_ID").cast(T.LongType()))
        .withColumn("offensiveRating", F.col("offensiveRating").cast(T.DoubleType()))
        .withColumn("defensiveRating", F.col("defensiveRating").cast(T.DoubleType()))
        .withColumn("netRating", F.col("netRating").cast(T.DoubleType()))
        .withColumn("usagePercentage", F.col("usagePercentage").cast(T.DoubleType()))
        .withColumn("trueShootingPercentage",
                     F.col("trueShootingPercentage").cast(T.DoubleType()))
        .withColumn("effectiveFieldGoalPercentage",
                     F.col("effectiveFieldGoalPercentage").cast(T.DoubleType()))
        .withColumn("PIE", F.col("PIE").cast(T.DoubleType()))
    )

    # ── Deduplication via window function ──
    w = Window.partitionBy("GAME_ID", "PERSON_ID").orderBy(F.lit(1))
    df = (
        df
        .withColumn("_row_num", F.row_number().over(w))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )

    _validate_boxscore(df, game_id)
    return df


def convert_pbp_spark(file_path: str) -> DataFrame:
    """Transform a single Bronze play-by-play JSON → Spark DataFrame."""
    spark = get_spark()
    game_id = os.path.basename(file_path).replace(".json", "")

    with open(file_path, "r") as f:
        data = json.load(f)

    try:
        actions = data["game"]["actions"]
    except (KeyError, TypeError):
        logger.warning("No actions in %s, skipping", file_path)
        return spark.createDataFrame([], PBP_SCHEMA)

    if not actions:
        return spark.createDataFrame([], PBP_SCHEMA)

    df = spark.createDataFrame(actions)
    df = df.withColumn("GAME_ID", F.lit(game_id))

    # ── Type casting ──
    df = (
        df
        .withColumn("period", F.col("period").cast(T.IntegerType()))
        .withColumn("teamId", F.col("teamId").cast(T.LongType()))
        .withColumn("personId", F.col("personId").cast(T.LongType()))
        .withColumn("shotDistance", F.col("shotDistance").cast(T.DoubleType()))
        .withColumn("scoreHome", F.col("scoreHome").cast(T.IntegerType()))
        .withColumn("scoreAway", F.col("scoreAway").cast(T.IntegerType()))
        .withColumn("isFieldGoal", F.col("isFieldGoal").cast(T.BooleanType()))
    )

    # ── UDF: Parse ISO 8601 clock → seconds ──
    df = df.withColumn("clock_seconds", parse_clock_udf(F.col("clock")))

    # ── Deduplication via window function ──
    w = Window.partitionBy("GAME_ID", "actionNumber").orderBy(F.lit(1))
    df = (
        df
        .withColumn("_row_num", F.row_number().over(w))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )

    _validate_pbp(df, game_id)
    return df


def convert_shotchart_spark(file_path: str) -> DataFrame:
    """Transform a single Bronze shot chart JSON → Spark DataFrame."""
    spark = get_spark()
    game_id = os.path.basename(file_path).replace(".json", "")

    with open(file_path, "r") as f:
        data = json.load(f)

    try:
        results = data["resultSets"][0]
    except (KeyError, IndexError, TypeError):
        logger.warning("No resultSets in %s, skipping", file_path)
        return spark.createDataFrame([], SHOTCHART_SCHEMA)

    headers = results["headers"]
    rows = results["rowSet"]
    if not rows:
        return spark.createDataFrame([], SHOTCHART_SCHEMA)

    # Build from rows + headers
    import pandas as pd
    pdf = pd.DataFrame(rows, columns=headers)
    df = spark.createDataFrame(pdf)
    df = df.withColumn("GAME_ID", F.lit(game_id))

    # ── Type casting ──
    df = (
        df
        .withColumn("PLAYER_ID", F.col("PLAYER_ID").cast(T.LongType()))
        .withColumn("TEAM_ID", F.col("TEAM_ID").cast(T.LongType()))
        .withColumn("PERIOD", F.col("PERIOD").cast(T.IntegerType()))
        .withColumn("LOC_X", F.col("LOC_X").cast(T.DoubleType()))
        .withColumn("LOC_Y", F.col("LOC_Y").cast(T.DoubleType()))
        .withColumn("SHOT_DISTANCE", F.col("SHOT_DISTANCE").cast(T.IntegerType()))
        .withColumn("SHOT_ATTEMPTED_FLAG", F.col("SHOT_ATTEMPTED_FLAG").cast(T.BooleanType()))
        .withColumn("SHOT_MADE_FLAG", F.col("SHOT_MADE_FLAG").cast(T.BooleanType()))
    )

    # ── Coordinate validation via Spark expressions ──
    df = df.withColumn(
        "_valid_coords",
        F.col("LOC_X").between(-250, 250) & F.col("LOC_Y").between(-50, 900)
    )

    invalid_count = df.filter(~F.col("_valid_coords")).count()
    if invalid_count > 0:
        logger.warning("shots/%s: %d rows with out-of-range coordinates", game_id, invalid_count)

    df = df.drop("_valid_coords")

    # ── Deduplication ──
    w = Window.partitionBy("GAME_ID", "GAME_EVENT_ID").orderBy(F.lit(1))
    df = (
        df
        .withColumn("_row_num", F.row_number().over(w))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )

    _validate_shotchart(df, game_id)
    return df


# ── Spark-native validation ─────────────────────────────────────────

_validation_results: list[dict] = []


def _validate_boxscore(df: DataFrame, game_id: str):
    """Validate boxscore DataFrame using Spark aggregations."""
    stats = df.agg(
        F.count("*").alias("total_rows"),
        F.sum(F.when(F.col("GAME_ID").isNull(), 1).otherwise(0)).alias("null_game_id"),
        F.sum(F.when(F.col("PERSON_ID").isNull(), 1).otherwise(0)).alias("null_person_id"),
        F.sum(F.when(F.col("TEAM_ID").isNull(), 1).otherwise(0)).alias("null_team_id"),
        F.sum(F.when(F.col("PIE") > 1, 1).otherwise(0)).alias("pie_above_1"),
        F.sum(F.when(F.col("usagePercentage") > 1, 1).otherwise(0)).alias("usg_above_1"),
    ).collect()[0]

    issues = []
    if stats.null_game_id > 0:
        issues.append(f"NULL: GAME_ID has {stats.null_game_id} nulls")
    if stats.null_person_id > 0:
        issues.append(f"NULL: PERSON_ID has {stats.null_person_id} nulls")
    if stats.null_team_id > 0:
        issues.append(f"NULL: TEAM_ID has {stats.null_team_id} nulls")
    if stats.total_rows < 10:
        issues.append(f"ROW COUNT: {stats.total_rows} rows (expected ≥10)")
    if stats.pie_above_1 > 0:
        issues.append(f"RANGE: PIE has {stats.pie_above_1} values above 1")
    if stats.usg_above_1 > 0:
        issues.append(f"RANGE: usagePercentage has {stats.usg_above_1} values above 1")

    passed = len(issues) == 0
    _validation_results.append({"source": f"boxscore/{game_id}", "passed": passed, "issues": issues})

    if passed:
        logger.info("  ✔ boxscore/%s: all checks passed", game_id)
    else:
        for issue in issues:
            logger.warning("  ⚠ boxscore/%s: %s", game_id, issue)


def _validate_pbp(df: DataFrame, game_id: str):
    """Validate play-by-play DataFrame using Spark aggregations."""
    stats = df.agg(
        F.count("*").alias("total_rows"),
        F.sum(F.when(F.col("GAME_ID").isNull(), 1).otherwise(0)).alias("null_game_id"),
        F.sum(F.when(F.col("actionNumber").isNull(), 1).otherwise(0)).alias("null_action"),
        F.sum(F.when(F.col("period").isNull(), 1).otherwise(0)).alias("null_period"),
        F.sum(F.when((F.col("period") < 1) | (F.col("period") > 10), 1)
              .otherwise(0)).alias("period_out_of_range"),
        F.sum(F.when(F.col("clock_seconds") > 720, 1).otherwise(0)).alias("clock_over_720"),
        F.sum(F.when(F.col("shotDistance") > 94, 1).otherwise(0)).alias("shot_over_94"),
    ).collect()[0]

    issues = []
    if stats.null_game_id > 0:
        issues.append(f"NULL: GAME_ID has {stats.null_game_id} nulls")
    if stats.null_action > 0:
        issues.append(f"NULL: actionNumber has {stats.null_action} nulls")
    if stats.total_rows < 100:
        issues.append(f"ROW COUNT: {stats.total_rows} rows (expected ≥100)")
    if stats.period_out_of_range > 0:
        issues.append(f"RANGE: period has {stats.period_out_of_range} out-of-range values")
    if stats.clock_over_720 > 0:
        issues.append(f"RANGE: clock_seconds has {stats.clock_over_720} values above 720")

    passed = len(issues) == 0
    _validation_results.append({"source": f"pbp/{game_id}", "passed": passed, "issues": issues})

    if passed:
        logger.info("  ✔ pbp/%s: all checks passed", game_id)
    else:
        for issue in issues:
            logger.warning("  ⚠ pbp/%s: %s", game_id, issue)


def _validate_shotchart(df: DataFrame, game_id: str):
    """Validate shot chart DataFrame using Spark aggregations."""
    stats = df.agg(
        F.count("*").alias("total_rows"),
        F.sum(F.when(F.col("GAME_ID").isNull(), 1).otherwise(0)).alias("null_game_id"),
        F.sum(F.when(F.col("GAME_EVENT_ID").isNull(), 1).otherwise(0)).alias("null_event_id"),
        F.sum(F.when(F.col("PLAYER_ID").isNull(), 1).otherwise(0)).alias("null_player_id"),
        F.sum(F.when((F.col("PERIOD") < 1) | (F.col("PERIOD") > 10), 1)
              .otherwise(0)).alias("period_out_of_range"),
        F.sum(F.when(F.col("SHOT_DISTANCE") > 94, 1).otherwise(0)).alias("shot_over_94"),
    ).collect()[0]

    issues = []
    if stats.null_game_id > 0:
        issues.append(f"NULL: GAME_ID has {stats.null_game_id} nulls")
    if stats.null_event_id > 0:
        issues.append(f"NULL: GAME_EVENT_ID has {stats.null_event_id} nulls")
    if stats.null_player_id > 0:
        issues.append(f"NULL: PLAYER_ID has {stats.null_player_id} nulls")
    if stats.period_out_of_range > 0:
        issues.append(f"RANGE: PERIOD has {stats.period_out_of_range} out-of-range values")

    passed = len(issues) == 0
    _validation_results.append({"source": f"shots/{game_id}", "passed": passed, "issues": issues})

    if passed:
        logger.info("  ✔ shots/%s: all checks passed", game_id)
    else:
        for issue in issues:
            logger.warning("  ⚠ shots/%s: %s", game_id, issue)


def print_validation_report():
    """Print a summary of all validation results for this run."""
    if not _validation_results:
        return

    passed = sum(1 for v in _validation_results if v["passed"])
    failed = sum(1 for v in _validation_results if not v["passed"])

    logger.info("─" * 50)
    logger.info("DATA VALIDATION REPORT (Spark)")
    logger.info("─" * 50)
    logger.info("  Checked: %d datasets", len(_validation_results))
    logger.info("  Passed:  %d", passed)
    if failed:
        logger.warning("  Failed:  %d", failed)
        for v in _validation_results:
            if not v["passed"]:
                for issue in v["issues"]:
                    logger.warning("    %s: %s", v["source"], issue)
    else:
        logger.info("  Failed:  0")
    logger.info("─" * 50)


# ── Orchestration ────────────────────────────────────────────────────

def process_date_spark(game_date: str):
    """
    Full Silver processing for one date using PySpark.

    Reads Bronze JSON files, transforms via Spark DataFrames,
    and writes partitioned Parquet to the Silver layer.
    """
    game_ids = load_manifest(game_date)

    if not game_ids:
        logger.info("No games on %s — nothing to extract.", game_date)
        return

    season = get_nba_season(game_date)
    logger.info("⚡ Processing %d games for %s (season %d) with Spark",
                len(game_ids), game_date, season)

    converters = [
        ("boxscores", convert_boxscore_spark, "boxscores"),
        ("pbp", convert_pbp_spark, "pbp"),
        ("shot_chart", convert_shotchart_spark, "shot_chart"),
    ]

    spark = get_spark()

    for dataset, converter, subfolder in converters:
        dfs = []
        for gid in game_ids:
            path = f"{BRONZE}/{subfolder}/{gid}.json"
            if os.path.exists(path):
                df = converter(path)
                if df.count() > 0:
                    dfs.append(df)

        if dfs:
            # Union all game DataFrames for this dataset
            combined = reduce(DataFrame.unionByName, dfs)
            row_count = combined.count()

            # Write partitioned Parquet
            out_dir = f"{SILVER}/{dataset}/season={season}/game_date={game_date}"
            os.makedirs(out_dir, exist_ok=True)
            (
                combined
                .coalesce(1)
                .write
                .mode("overwrite")
                .parquet(out_dir)
            )

            # Rename Spark's part-* file to data.parquet for compatibility
            _rename_spark_output(out_dir)
            logger.info("%s/data.parquet → %d rows", out_dir, row_count)
        else:
            logger.warning("No %s data for %s", dataset, game_date)


def _rename_spark_output(out_dir: str):
    """
    Spark writes part-00000-*.parquet files. Rename to data.parquet
    to stay compatible with the pandas version and Gold layer readers.
    """
    for f in os.listdir(out_dir):
        if f.startswith("part-") and f.endswith(".parquet"):
            os.rename(
                os.path.join(out_dir, f),
                os.path.join(out_dir, "data.parquet"),
            )
        elif f.startswith(".") or f == "_SUCCESS":
            os.remove(os.path.join(out_dir, f))


# ── CLI ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    parser = argparse.ArgumentParser(
        description="Transform Bronze JSON → Silver Parquet using PySpark."
    )
    parser.add_argument("game_date", help="YYYY-MM-DD")
    parser.add_argument("--dims", action="store_true",
                        help="Also process dimension tables (players + teams).")
    args = parser.parse_args()

    logger.info("=" * 50)
    logger.info("Bronze → Silver (Spark): %s", args.game_date)
    logger.info("=" * 50)

    if args.dims:
        # Dimensions stay pandas-based (small data, no Spark benefit)
        from nba_etl.silver.extraction import process_players, process_teams
        process_players()
        process_teams()

    process_date_spark(args.game_date)

    print_validation_report()
    _validation_results.clear()
    logger.info("Done — Silver parquets written via Spark.")
