import os 
import pandas as pd
import json
import glob

BRONZE_PATH = "/app/datalake/bronze"
SILVER_PATH = "/app/datalake/silver"

for folder in ["teams", "players", "pbp", "boxscores", "shot_chart"]:
    os.makedirs(f"{SILVER_PATH}/{folder}", exist_ok=True)


# ── Helpers ──────────────────────────────────────────────────────────

def get_game_id_from_filename(file_path):
    """Extracts '0022300001' from '/.../0022300001.json'"""
    return os.path.basename(file_path).replace('.json', '')


# ── Converters ───────────────────────────────────────────────────────

def convert_players_pd() -> pd.DataFrame:
    """Load the players JSON list and return a cleaned DataFrame."""
    file_path = f"{BRONZE_PATH}/players/players.json"
    with open(file_path, 'r') as f:
        data = json.load(f)        # list of dicts

    df = pd.DataFrame(data)

    # ── Type casting ──
    df['id'] = df['id'].astype(int)
    df['is_active'] = df['is_active'].astype(bool)
    df['full_name'] = df['full_name'].astype(str).str.strip()
    df['first_name'] = df['first_name'].astype(str).str.strip()
    df['last_name'] = df['last_name'].astype(str).str.strip()

    # ── Deduplication ──
    df = df.drop_duplicates(subset=['id'])

    return df


def convert_teams_pd(file_path: str) -> pd.DataFrame:
    """Convert a single team game-log JSON (resultSets format) to DataFrame."""
    with open(file_path, 'r') as f:
        data = json.load(f)

    headers = data['resultSets'][0]['headers']
    rows = data['resultSets'][0]['rowSet']
    df = pd.DataFrame(rows, columns=headers)

    # ── Type casting ──
    df['TEAM_ID'] = df['TEAM_ID'].astype(int)
    df['PTS'] = pd.to_numeric(df['PTS'], errors='coerce').astype('Int64')
    df['PLUS_MINUS'] = pd.to_numeric(df['PLUS_MINUS'], errors='coerce')
    for col in ['FG_PCT', 'FG3_PCT', 'FT_PCT']:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    for col in ['FGM', 'FGA', 'FG3M', 'FG3A', 'FTM', 'FTA',
                'OREB', 'DREB', 'REB', 'AST', 'STL', 'BLK', 'TOV', 'PF']:
        df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')

    # ── Date formatting ──
    df['GAME_DATE'] = pd.to_datetime(df['GAME_DATE'], errors='coerce')

    # ── Deduplication ──
    df = df.drop_duplicates(subset=['GAME_ID', 'TEAM_ID'])

    return df


def convert_pbp_pd(file_path: str) -> pd.DataFrame:
    """Convert a V3 play-by-play JSON to DataFrame."""
    game_id = get_game_id_from_filename(file_path)

    with open(file_path, 'r') as f:
        data = json.load(f)

    # V3 structure: data['game']['actions']
    try:
        actions = data['game']['actions']
    except (KeyError, TypeError):
        print(f"  ⚠ No actions in {file_path}, skipping.")
        return pd.DataFrame()

    if not actions:
        return pd.DataFrame()

    df = pd.DataFrame(actions)

    # ── Inject Game ID ──
    df['GAME_ID'] = game_id

    # ── Type casting ──
    df['period'] = pd.to_numeric(df['period'], errors='coerce').astype('Int64')
    df['teamId'] = pd.to_numeric(df['teamId'], errors='coerce').astype('Int64')
    df['personId'] = pd.to_numeric(df['personId'], errors='coerce').astype('Int64')
    df['shotDistance'] = pd.to_numeric(df['shotDistance'], errors='coerce')
    df['isFieldGoal'] = df['isFieldGoal'].astype(bool)
    df['scoreHome'] = pd.to_numeric(df['scoreHome'], errors='coerce').astype('Int64')
    df['scoreAway'] = pd.to_numeric(df['scoreAway'], errors='coerce').astype('Int64')

    # ── Parse clock string (e.g. "PT12M00.00S") to seconds remaining ──
    def parse_clock(clock_str):
        """Convert 'PT12M00.00S' to total seconds (float)."""
        if not isinstance(clock_str, str) or not clock_str.startswith('PT'):
            return None
        try:
            time_part = clock_str[2:]  # strip 'PT'
            minutes, rest = time_part.split('M')
            seconds = rest.rstrip('S')
            return float(minutes) * 60 + float(seconds)
        except (ValueError, AttributeError):
            return None

    df['clock_seconds'] = df['clock'].apply(parse_clock)

    # ── Deduplication (by game + action number) ──
    df = df.drop_duplicates(subset=['GAME_ID', 'actionNumber'])

    return df


def convert_boxscores_pd(file_path: str) -> pd.DataFrame:
    """Convert a V3 advanced boxscore JSON to a flat DataFrame of player stats."""
    game_id = get_game_id_from_filename(file_path)

    with open(file_path, 'r') as f:
        data = json.load(f)

    # V3 key is 'boxScoreAdvanced', not 'boxScoreTraditional'
    try:
        bs = data['boxScoreAdvanced']
    except (KeyError, TypeError):
        print(f"  ⚠ No boxScoreAdvanced in {file_path}, skipping.")
        return pd.DataFrame()

    all_players = []
    for team_type in ['homeTeam', 'awayTeam']:
        team_data = bs.get(team_type, {})
        team_id = team_data.get('teamId')
        team_tricode = team_data.get('teamTricode', '')

        for player in team_data.get('players', []):
            # Flatten the nested 'statistics' dict into the player row
            row = {
                'PERSON_ID': player.get('personId'),
                'FIRST_NAME': player.get('firstName', ''),
                'FAMILY_NAME': player.get('familyName', ''),
                'NAME_I': player.get('nameI', ''),
                'POSITION': player.get('position', ''),
                'COMMENT': player.get('comment', ''),
                'JERSEY_NUM': player.get('jerseyNum', ''),
                'TEAM_ID': team_id,
                'TEAM_TRICODE': team_tricode,
                'TEAM_TYPE': 'HOME' if team_type == 'homeTeam' else 'AWAY',
            }
            # Pull all statistics into flat columns
            stats = player.get('statistics', {})
            for stat_key, stat_val in stats.items():
                row[stat_key] = stat_val

            all_players.append(row)

    if not all_players:
        return pd.DataFrame()

    df = pd.DataFrame(all_players)

    # ── Inject Game ID ──
    df['GAME_ID'] = game_id

    # ── Type casting ──
    df['PERSON_ID'] = pd.to_numeric(df['PERSON_ID'], errors='coerce').astype('Int64')
    df['TEAM_ID'] = pd.to_numeric(df['TEAM_ID'], errors='coerce').astype('Int64')

    # Cast all numeric stat columns
    stat_cols = [c for c in df.columns if c not in (
        'GAME_ID', 'PERSON_ID', 'FIRST_NAME', 'FAMILY_NAME', 'NAME_I',
        'POSITION', 'COMMENT', 'JERSEY_NUM', 'TEAM_ID', 'TEAM_TRICODE',
        'TEAM_TYPE', 'minutes')]
    for col in stat_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    # ── Deduplication ──
    df = df.drop_duplicates(subset=['GAME_ID', 'PERSON_ID'])

    return df


def convert_shotchart_pd(file_path: str) -> pd.DataFrame:
    """Convert a shot chart JSON (resultSets format) to DataFrame."""
    game_id = get_game_id_from_filename(file_path)

    with open(file_path, 'r') as f:
        data = json.load(f)

    try:
        results = data['resultSets'][0]
    except (KeyError, IndexError, TypeError):
        print(f"  ⚠ No resultSets in {file_path}, skipping.")
        return pd.DataFrame()

    df = pd.DataFrame(results['rowSet'], columns=results['headers'])

    # Ensure GAME_ID is set
    df['GAME_ID'] = game_id

    # ── Type casting ──
    df['PLAYER_ID'] = pd.to_numeric(df['PLAYER_ID'], errors='coerce').astype('Int64')
    df['TEAM_ID'] = pd.to_numeric(df['TEAM_ID'], errors='coerce').astype('Int64')
    df['PERIOD'] = pd.to_numeric(df['PERIOD'], errors='coerce').astype('Int64')
    df['SHOT_DISTANCE'] = pd.to_numeric(df['SHOT_DISTANCE'], errors='coerce')
    df['LOC_X'] = pd.to_numeric(df['LOC_X'], errors='coerce')
    df['LOC_Y'] = pd.to_numeric(df['LOC_Y'], errors='coerce')
    df['SHOT_ATTEMPTED_FLAG'] = df['SHOT_ATTEMPTED_FLAG'].astype(bool)
    df['SHOT_MADE_FLAG'] = df['SHOT_MADE_FLAG'].astype(bool)

    # ── Date formatting ──
    df['GAME_DATE'] = pd.to_datetime(df['GAME_DATE'], format='%Y%m%d', errors='coerce')

    # ── Deduplication (by game + event id) ──
    df = df.drop_duplicates(subset=['GAME_ID', 'GAME_EVENT_ID'])

    return df


# ── Batch processors: read all Bronze → combine → save Silver Parquet ──

def process_players():
    print("Processing players...")
    df = convert_players_pd()
    print(f"  {len(df)} players")
    df.to_parquet(f"{SILVER_PATH}/players/players.parquet", index=False)
    print(f"  ✔ Saved to {SILVER_PATH}/players/players.parquet")
    return df


def process_teams():
    print("Processing teams...")
    team_files = glob.glob(f"{BRONZE_PATH}/teams/*.json")
    frames = []
    for tf in sorted(team_files):
        try:
            frames.append(convert_teams_pd(tf))
        except Exception as e:
            print(f"  ⚠ Error in {tf}: {e}")

    df = pd.concat(frames, ignore_index=True)
    df = df.drop_duplicates(subset=['GAME_ID', 'TEAM_ID'])
    print(f"  {len(df)} team-game rows from {len(team_files)} team files")
    df.to_parquet(f"{SILVER_PATH}/teams/team_game_logs.parquet", index=False)
    print(f"  ✔ Saved to {SILVER_PATH}/teams/team_game_logs.parquet")
    return df


def process_boxscores():
    print("Processing boxscores...")
    box_files = glob.glob(f"{BRONZE_PATH}/boxscores/*.json")
    frames = []
    for bf in sorted(box_files):
        try:
            df = convert_boxscores_pd(bf)
            if not df.empty:
                frames.append(df)
        except Exception as e:
            print(f"  ⚠ Error in {bf}: {e}")

    if not frames:
        print("  ⚠ No boxscore data to process.")
        return pd.DataFrame()

    df = pd.concat(frames, ignore_index=True)
    df = df.drop_duplicates(subset=['GAME_ID', 'PERSON_ID'])
    print(f"  {len(df)} player-game rows from {len(box_files)} games")
    df.to_parquet(f"{SILVER_PATH}/boxscores/boxscores.parquet", index=False)
    print(f"  ✔ Saved to {SILVER_PATH}/boxscores/boxscores.parquet")
    return df


def process_pbp():
    print("Processing play-by-play...")
    pbp_files = glob.glob(f"{BRONZE_PATH}/pbp/*.json")
    frames = []
    for pf in sorted(pbp_files):
        try:
            df = convert_pbp_pd(pf)
            if not df.empty:
                frames.append(df)
        except Exception as e:
            print(f"  ⚠ Error in {pf}: {e}")

    if not frames:
        print("  ⚠ No PBP data to process.")
        return pd.DataFrame()

    df = pd.concat(frames, ignore_index=True)
    df = df.drop_duplicates(subset=['GAME_ID', 'actionNumber'])
    print(f"  {len(df)} play-by-play rows from {len(pbp_files)} games")
    df.to_parquet(f"{SILVER_PATH}/pbp/pbp.parquet", index=False)
    print(f"  ✔ Saved to {SILVER_PATH}/pbp/pbp.parquet")
    return df


def process_shotcharts():
    print("Processing shot charts...")
    sc_files = glob.glob(f"{BRONZE_PATH}/shot_chart/*.json")
    frames = []
    for sf in sorted(sc_files):
        try:
            df = convert_shotchart_pd(sf)
            if not df.empty:
                frames.append(df)
        except Exception as e:
            print(f"  ⚠ Error in {sf}: {e}")

    if not frames:
        print("  ⚠ No shot chart data to process.")
        return pd.DataFrame()

    df = pd.concat(frames, ignore_index=True)
    df = df.drop_duplicates(subset=['GAME_ID', 'GAME_EVENT_ID'])
    print(f"  {len(df)} shot rows from {len(sc_files)} games")
    df.to_parquet(f"{SILVER_PATH}/shot_chart/shot_charts.parquet", index=False)
    print(f"  ✔ Saved to {SILVER_PATH}/shot_chart/shot_charts.parquet")
    return df


# ── Main ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 50)
    print("Bronze → Silver Data Extraction")
    print("=" * 50)

    process_players()
    process_teams()
    process_boxscores()
    process_shotcharts()
    process_pbp()

    print()
    print("=" * 50)
    print("Done! Silver layer parquets written.")
    print("=" * 50)