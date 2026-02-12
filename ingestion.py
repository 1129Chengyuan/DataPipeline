import os
import pandas as pd
import json
import time
import requests
from nba_api.stats.static import teams, players
from nba_api.stats.endpoints import leaguegamefinder, shotchartdetail

# Headers required by stats.nba.com to avoid 403 errors
NBA_HEADERS = {
    'Host': 'stats.nba.com',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'en-US,en;q=0.5',
    'Referer': 'https://stats.nba.com',
    'x-nba-stats-origin': 'stats',
    'x-nba-stats-token': 'true',
    'Connection': 'keep-alive',
}

def _fetch_nba_stats(endpoint, params):
    """Make a direct HTTP request to stats.nba.com, bypassing nba_api's broken parser."""
    url = f'https://stats.nba.com/stats/{endpoint}'
    response = requests.get(url, headers=NBA_HEADERS, params=params, timeout=30)
    response.raise_for_status()
    return response.json()

# First create the folders
BRONZE_PATH = "/app/datalake/bronze"
for folder in ["teams", "players", "pbp", "boxscores", "shot_chart"]:
    os.makedirs(f"{BRONZE_PATH}/{folder}", exist_ok=True)

def get_all_teams():
    all_teams = teams.get_teams()
    for team in all_teams:
        team_id = team['id']
        team_name = team['full_name']

        team_file = f"{BRONZE_PATH}/teams/{team_id}.json"

        if os.path.exists(team_file):
            print(f"{team_name} already downloaded.")
            continue
        print(f"Downloading history for {team_name}")

        try:
            game_finder = leaguegamefinder.LeagueGameFinder(team_id_nullable = team_id)
            data = game_finder.get_dict()
            with open(team_file, "w") as f:
                json.dump(data, f)
            print(f"Saved {team_name} successfully.") 
            time.sleep(0.600)
        except Exception as e:
            print(f"Failed to download {team_name}: {e}")
    return all_teams

def get_all_players():
    unique_game_ids = set()
    players_df = players.get_players()
    player_file = f"{BRONZE_PATH}/players/players.json"
    try:
        with open(player_file, "w") as f:
            json.dump(players_df, f)
        print(f"Saved players successfully.")
    except Exception as e:
        print(f"Failed to download players: {e}")

def get_playbyplay(unique_game_ids):
    for game_id in unique_game_ids:
        pbp_file = f"{BRONZE_PATH}/pbp/{game_id}.json"
        if os.path.exists(pbp_file):
            print(f"Game {game_id} already downloaded.")
            continue
        print(f"Downloading play-by-play for game {game_id}")
        try:
            data = _fetch_nba_stats('playbyplayv2', {
                'GameID': game_id,
                'StartPeriod': 0,
                'EndPeriod': 0,
            })
            with open(pbp_file, "w") as f:
                json.dump(data, f)
            print(f"Saved play-by-play for game {game_id} successfully.")
            time.sleep(0.600)
        except Exception as e:
            print(f"Failed to download play-by-play for game {game_id}: {e}")

def get_games(all_teams) -> set:
    unique_game_ids = set()
    
    team_files = os.listdir(f"{BRONZE_PATH}/teams")
    
    for filename in team_files:
        if not filename.endswith(".json"): continue
        
        file_path = f"{BRONZE_PATH}/teams/{filename}"
        
        try:
            with open(file_path, "r") as f:
                data = json.load(f)
            
            if 'resultSets' in data and len(data['resultSets']) > 0:
                headers = data['resultSets'][0]['headers']
                rows = data['resultSets'][0]['rowSet']
                if 'GAME_ID' in headers:
                    idx = headers.index('GAME_ID')
                    for row in rows:
                        unique_game_ids.add(row[idx])
        except Exception as e:
            print(f"Error reading {filename}: {e}")
            
    print(f"Found {len(unique_game_ids)} unique games in history.")
    return unique_game_ids

def get_boxscores(unique_game_ids):
    for game_id in unique_game_ids:
        boxscore_file = f"{BRONZE_PATH}/boxscores/{game_id}.json"
        if os.path.exists(boxscore_file):
            print(f"Boxscore for game {game_id} already downloaded.")
            continue
        print(f"Downloading boxscore for game {game_id}")
        try:
            data = _fetch_nba_stats('boxscoreadvancedv2', {
                'GameID': game_id,
                'StartPeriod': 0,
                'EndPeriod': 0,
                'StartRange': 0,
                'EndRange': 0,
                'RangeType': 0,
            })
            with open(boxscore_file, "w") as f:
                json.dump(data, f)
            print(f"Saved boxscore for game {game_id} successfully.")
            time.sleep(0.600)
        except Exception as e:
            print(f"Failed to download boxscore for game {game_id}: {e}")

def get_shotcharts(unique_game_ids):
    for game_id in unique_game_ids:
        shotchart_file = f"{BRONZE_PATH}/shot_chart/{game_id}.json"
        if not os.path.exists(shotchart_file):
            try:
                shotchart = shotchartdetail.ShotChartDetail(
                    team_id=0,
                    player_id=0,
                    game_id_nullable=game_id,
                    context_measure_simple='FGA')
                data = shotchart.get_dict()
                with open(shotchart_file, "w") as f:
                    json.dump(data, f)
                print(f"Saved shotchart for {game_id} successfully.")
                time.sleep(0.600)
            except Exception as e:
                print(f"Failed to download shotchart for game {game_id}: {e}")

if __name__ == "__main__":
    all_teams = get_all_teams()
    unique_game_ids = get_games(all_teams)
    # just test with 5 games first
    recent_games = [gid for gid in unique_game_ids if gid.startswith('002')]
    unique_game_ids = recent_games[-10:]
    get_boxscores(unique_game_ids)
    get_shotcharts(unique_game_ids)
    get_playbyplay(unique_game_ids)
    get_all_players()
