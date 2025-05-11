# fetchNBA_stats_comprehensive.py

import pandas as pd
from nba_api.stats.static import players as nba_static_players
from nba_api.stats.endpoints import playergamelog, commonplayerinfo, playerprofilev2
from nba_api.stats.library.parameters import SeasonAll
import os
import time
import logging
from readPL import read_player_list # Assuming readPL.py is in the same directory or PYTHONPATH
import json

# --- Configuration ---
# Seasons to fetch data for. Example: "2023-24", "2022-23".
# The API often uses "YYYY" for the start year of the season, e.g., "2023" for "2023-24".
# Let's adapt to the "YYYY" format for PlayerGameLog season parameter.
SEASONS_TO_FETCH = ["2022","2023", "2024"] # For 2023-24 and 2022-23 seasons respectively

# Output directory for raw player stats
RAW_STATS_DIR = 'data/new/player_stats'
PLAYER_ID_CACHE_FILE = os.path.join(RAW_STATS_DIR, 'player_id_cache.json') # To store player_name:id mapping

# API Call Configuration
REQUEST_TIMEOUT = 30  # seconds
RETRY_ATTEMPTS = 3
RETRY_DELAY = 5  # seconds (base delay, will increase)
API_CALL_DELAY = 1.0 # seconds between API calls to be polite

# Logging Setup
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[logging.FileHandler("data_acquisition_nba.log"),
                              logging.StreamHandler()])

# --- Helper Functions ---
def load_player_id_cache():
    if os.path.exists(PLAYER_ID_CACHE_FILE):
        try:
            with open(PLAYER_ID_CACHE_FILE, 'r') as f:
                return json.load(f)
        except json.JSONDecodeError:
            logging.warning(f"Could not decode JSON from {PLAYER_ID_CACHE_FILE}. Starting with an empty cache.")
            return {}
    return {}

def save_player_id_cache(cache):
    os.makedirs(os.path.dirname(PLAYER_ID_CACHE_FILE), exist_ok=True)
    with open(PLAYER_ID_CACHE_FILE, 'w') as f:
        json.dump(cache, f, indent=4)

# Global cache for player IDs
PLAYER_ID_CACHE = load_player_id_cache()
ALL_NBA_PLAYERS_LIST = None # Cache for all players list from API

def get_all_nba_players_cached():
    """Fetches and caches the list of all NBA players from the API."""
    global ALL_NBA_PLAYERS_LIST
    if ALL_NBA_PLAYERS_LIST is None:
        logging.info("Fetching list of all NBA players from API...")
        try:
            ALL_NBA_PLAYERS_LIST = nba_static_players.get_players() # Fetches active and inactive
            time.sleep(API_CALL_DELAY) # Be polite
        except Exception as e:
            logging.error(f"Failed to fetch all NBA players list: {e}")
            ALL_NBA_PLAYERS_LIST = [] # Return empty list on failure to avoid repeated calls
    return ALL_NBA_PLAYERS_LIST


def get_player_id(player_name):
    """
    Retrieves player ID, first from cache, then by searching the API.
    Uses a more robust full name matching.
    """
    if player_name in PLAYER_ID_CACHE:
        return PLAYER_ID_CACHE[player_name]

    all_players_list = get_all_nba_players_cached()
    if not all_players_list:
        logging.warning(f"Cannot search for player ID for {player_name} as the all players list is empty.")
        return None

    # Prioritize exact full name match
    for p in all_players_list:
        if p['full_name'].lower() == player_name.lower():
            PLAYER_ID_CACHE[player_name] = p['id']
            save_player_id_cache(PLAYER_ID_CACHE)
            return p['id']

    # Fallback: simple substring match (can be ambiguous, use with caution or refine)
    # For your list, exact match should be fine.
    # matched_players = [p for p in all_players_list if player_name.lower() in p['full_name'].lower()]
    # if len(matched_players) == 1:
    #     p = matched_players[0]
    #     logging.info(f"Found player {p['full_name']} (ID: {p['id']}) for search term '{player_name}' (substring match).")
    #     PLAYER_ID_CACHE[player_name] = p['id']
    #     save_player_id_cache(PLAYER_ID_CACHE)
    #     return p['id']
    # elif len(matched_players) > 1:
    #     logging.warning(f"Ambiguous player name '{player_name}'. Found multiple matches: {[mp['full_name'] for mp in matched_players]}. Please be more specific.")
    #     return None

    logging.warning(f"Player ID not found for '{player_name}'.")
    return None

def make_api_request(endpoint_callable, **kwargs):
    """Makes an API request with retries and timeout."""
    for attempt in range(RETRY_ATTEMPTS):
        try:
            logging.debug(f"Attempt {attempt + 1} for {endpoint_callable.__name__} with args {kwargs}")
            data = endpoint_callable(**kwargs, timeout=REQUEST_TIMEOUT).get_data_frames()
            time.sleep(API_CALL_DELAY) # Be polite after a successful call
            return data
        except Exception as e:
            logging.warning(f"API request failed for {endpoint_callable.__name__} (Attempt {attempt + 1}/{RETRY_ATTEMPTS}): {e}")
            if attempt < RETRY_ATTEMPTS - 1:
                delay = RETRY_DELAY * (2 ** attempt) # Exponential backoff
                logging.info(f"Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                logging.error(f"All retry attempts failed for {endpoint_callable.__self__.__class__.__name__}.")
                return None # Or raise the exception: raise e

def fetch_player_game_logs(player_id, player_name, season_year_str):
    """Fetches and saves player game logs for a specific season."""
    logging.info(f"Fetching game logs for {player_name} (ID: {player_id}) for season {season_year_str}...")
    # The API uses 'YYYY' for season, e.g., "2023" for the 2023-24 season.
    # If your SEASONS_TO_FETCH is "2023-24", you'd pass "2023" to the API.
    # Our SEASONS_TO_FETCH is already in "YYYY" format.

    gamelog_data_frames = make_api_request(playergamelog.PlayerGameLog,
                                           player_id=player_id,
                                           season=season_year_str,
                                           season_type_all_star="Regular Season") # Or "Playoffs", "All Star", etc.

    if gamelog_data_frames and len(gamelog_data_frames) > 0:
        gamelog_df = gamelog_data_frames[0]
        if not gamelog_df.empty:
            # Create season-specific subdirectory
            season_output_dir = os.path.join(RAW_STATS_DIR, f"season_{season_year_str}")
            os.makedirs(season_output_dir, exist_ok=True)

            file_name = os.path.join(season_output_dir, f"{player_name.replace(' ', '_').lower()}_gamelog.csv")
            gamelog_df.to_csv(file_name, index=False)
            logging.info(f"Saved {len(gamelog_df)} games to {file_name}")
            return gamelog_df
        else:
            logging.info(f"No game logs found for {player_name} for season {season_year_str}.")
            return pd.DataFrame() # Return empty DataFrame
    else:
        logging.warning(f"Failed to fetch game logs for {player_name} for season {season_year_str}.")
        return pd.DataFrame()

# --- Optional: Other data points you might want ---
def fetch_player_common_info(player_id, player_name):
    """Fetches common player info (birthdate, height, weight, etc.)."""
    logging.info(f"Fetching common info for {player_name} (ID: {player_id})...")
    info_data_frames = make_api_request(commonplayerinfo.CommonPlayerInfo, player_id=player_id)

    if info_data_frames and len(info_data_frames) > 0:
        info_df = info_data_frames[0] # Usually the first DataFrame has the main info
        if not info_df.empty:
            os.makedirs(RAW_STATS_DIR, exist_ok=True) # Ensure base directory exists
            file_name = os.path.join(RAW_STATS_DIR, f"{player_name.replace(' ', '_').lower()}_commoninfo.csv")
            info_df.to_csv(file_name, index=False)
            logging.info(f"Saved common info to {file_name}")
            return info_df
    logging.warning(f"Failed to fetch common info for {player_name}.")
    return pd.DataFrame()


def fetch_player_advanced_stats_per_season(player_id, player_name, season_year_str):
    """
    Fetches advanced stats for a player for a specific season.
    PlayerProfileV2 is a rich endpoint but can be complex.
    It often returns stats split by regular season, playoffs, etc.
    """
    logging.info(f"Fetching advanced stats for {player_name} (ID: {player_id}) for season {season_year_str}...")
    # The season format for PlayerProfileV2 might be "YYYY-YY", e.g., "2023-24"
    # Let's construct that from our "YYYY" format.
    next_year_short = str(int(season_year_str) + 1)[-2:]
    season_api_format = f"{season_year_str}-{next_year_short}"

    profile_data_frames = make_api_request(playerprofilev2.PlayerProfileV2,
                                           player_id=player_id,
                                           per_mode36="PerGame") # Or "Totals", "PerMinute", etc.

    if profile_data_frames:
        # PlayerProfileV2 returns many DataFrames.
        # 'SeasonTotalsRegularSeason' or similar is often what we want.
        # You'll need to inspect the returned DataFrames to find the correct one.
        # Example: Find DF that has a 'SEASON_ID' column and matches your target season.
        target_df = None
        for i, df in enumerate(profile_data_frames):
            if 'SEASON_ID' in df.columns and not df[df['SEASON_ID'] == season_api_format].empty:
                 # Check if it's regular season data if multiple tables match season
                if "Regular Season" in df.name if hasattr(df, 'name') else True: # Heuristic
                    target_df = df[df['SEASON_ID'] == season_api_format]
                    logging.debug(f"Found advanced stats in DataFrame index {i} for season {season_api_format}")
                    break
            # Alternative: Check for typical advanced stats columns
            # typical_adv_cols = {'AST_PCT', 'REB_PCT', 'USG_PCT', 'PIE'}
            # if 'SEASON_ID' in df.columns and typical_adv_cols.issubset(df.columns) and not df[df['SEASON_ID'] == season_api_format].empty:
            #     target_df = df[df['SEASON_ID'] == season_api_format]
            #     logging.debug(f"Found potential advanced stats in DataFrame index {i} for season {season_api_format} based on columns.")
            #     break


        if target_df is not None and not target_df.empty:
            season_output_dir = os.path.join(RAW_STATS_DIR, f"season_{season_year_str}")
            os.makedirs(season_output_dir, exist_ok=True)
            file_name = os.path.join(season_output_dir, f"{player_name.replace(' ', '_').lower()}_advanced_stats.csv")
            target_df.to_csv(file_name, index=False)
            logging.info(f"Saved advanced stats to {file_name}")
            return target_df
        else:
            logging.info(f"No specific advanced stats DataFrame found for {player_name} for season {season_api_format} in PlayerProfileV2 output.")
            return pd.DataFrame()
    else:
        logging.warning(f"Failed to fetch player profile (advanced stats) for {player_name} for season {season_api_format}.")
        return pd.DataFrame()

# --- Main Execution ---
if __name__ == "__main__":
    # Ensure the main raw stats directory exists
    os.makedirs(RAW_STATS_DIR, exist_ok=True)

    # Load player list from players.txt
    target_player_names = read_player_list()
    if not target_player_names:
        logging.error("No players found in players.txt. Exiting.")
        exit()

    logging.info(f"Starting NBA data acquisition for {len(target_player_names)} players and {len(SEASONS_TO_FETCH)} seasons.")

    # Pre-populate player ID cache for all target players if desired
    logging.info("Pre-caching player IDs for target list...")
    for player_name_from_file in target_player_names:
        get_player_id(player_name_from_file) # This will fetch and cache if not present
    save_player_id_cache(PLAYER_ID_CACHE) # Save any newly fetched IDs
    logging.info("Player ID caching complete.")


    for player_name in target_player_names:
        player_id = get_player_id(player_name) # Get from cache or fetch

        if not player_id:
            logging.warning(f"Skipping {player_name} as ID could not be found.")
            continue

        logging.info(f"--- Processing Player: {player_name} (ID: {player_id}) ---")

        # Optional: Fetch common player info (once per player, not per season)
        # fetch_player_common_info(player_id, player_name)
        # time.sleep(API_CALL_DELAY) # Pause after common info call

        for season_year in SEASONS_TO_FETCH:
            logging.info(f"--- Season: {season_year} ({int(season_year)}-{int(season_year)+1}) ---")

            # Fetch Game Logs (Primary Data)
            fetch_player_game_logs(player_id, player_name, season_year)
            time.sleep(API_CALL_DELAY) # Pause

            # Optional: Fetch Advanced Stats per season
            # fetch_player_advanced_stats_per_season(player_id, player_name, season_year)
            # time.sleep(API_CALL_DELAY) # Pause

    logging.info("NBA data acquisition process finished.")