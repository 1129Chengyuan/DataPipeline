import os
import pandas as pd
import json
import time
from nba_api.stats.static import teams, players
from nba_api.stats.endpoints import leaguegamefinder

# First create the folders
BRONZE_PATH = "/app/datalake/bronze"
os.makedirs(BRONZE_PATH, exist_ok=True)


# for the teams
os.makedirs(BRONZE_PATH + "/teams", exist_ok=True)
# get all teams in a list of dicts 
all_teams = teams.get_teams()
for team in all_teams:
    # find the team id and name
    team_id = team['id']
    team_name = team['full_name']

    # filename : /app/datalake/bronze/0000.json
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
    

# now for the players
os.makedirs(BRONZE_PATH + "/players", exist_ok = True)
players_df = players.get_players()
player_file = f"{BRONZE_PATH}/players/players.json"
try:
    with open(player_file, "w") as f:
        json.dump(players_df, f)
    print(f"Saved players successfully.")
except Exception as e:
    print(f"Failed to download players: {e}")
