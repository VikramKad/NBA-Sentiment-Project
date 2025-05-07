import pandas as pd
from nba_api.stats.static import players
from nba_api.stats.endpoints import playergamelog
import os

def get_player_id(player_name):
    all_players = players.get_active_players()
    player = next((p for p in all_players if p['full_name'].lower() == player_name.lower()), None)
    return player['id'] if player else None

def fetch_and_save_stats(player_name, season='2023'):
    player_id = get_player_id(player_name)
    if not player_id:
        print(f"Player {player_name} not found.")
        return

    print(f"Fetching game log for {player_name} ({season} season)...")
    gamelog = playergamelog.PlayerGameLog(player_id=player_id, season=season).get_data_frames()[0]

    # Make sure data folder exists
    os.makedirs('data', exist_ok=True)
    file_name = f"data/{player_name.replace(' ', '_').lower()}_stats.csv"
    gamelog.to_csv(file_name, index=False)
    print(f"Saved {len(gamelog)} games to {file_name}")

def read_player_list(file_path='players.txt'):
    with open(file_path, 'r') as f:
        players = [line.strip() for line in f if line.strip()]
    return players

if __name__ == "__main__":
    season = "2024"

    player_list = read_player_list()

    for player_name in player_list:
        fetch_and_save_stats(player_name, season)
