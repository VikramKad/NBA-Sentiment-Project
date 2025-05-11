# fetch_reddit_data_comprehensive.py

import praw
import pandas as pd
import os
import json
import time
import logging
from datetime import datetime, timedelta
from readPL import read_player_list

# --- Configuration ---
REDDIT_CLIENT_ID = 'E2MfNNRsw6F5YfvVoc8Jtg' 
REDDIT_CLIENT_SECRET = 'Ns86Hi0A_jcWH2TwzWE5JBGsXZKBLw' 
REDDIT_USER_AGENT = 'NBA Sent Gatherer by /u/Long-Place-9308' 

PLAYER_STATS_BASE_DIR = 'data/new/player_stats'
PLAYER_NICKNAMES_FILE = 'data/json/nicknames.json' # Ensure this file exists and is correct!
REDDIT_OUTPUT_DIR = 'data/new/reddit_data'

PLAYER_TEAM_SUBREDDITS = {
    "LeBron James": "lakers",
    "Stephen Curry": "warriors",
    "Jalen Brunson": "NYKnicks",
    "Giannis Antetokounmpo": "MkeBucks",
    "Luka Doncic": "lakers", 
    "Shai Gilgeous-Alexander": "Thunder",
    "Anthony Edwards": "timberwolves",
    "Donovan Mitchell": "clevelandcavs"
}
GENERAL_SUBREDDITS = ['nba', 'nbadiscussion']

POST_SEARCH_LIMIT_PER_QUERY = 75 # Can increase slightly if using OR queries
COMMENT_LIMIT_PER_POST = 30     # Reduced for speed, adjust if more needed
MIN_POST_SCORE_FOR_DEEP_COMMENTS = 5 # Only fetch comments for posts with at least this score
FETCH_DEEP_COMMENTS = True # Set to False for top-level only, much faster

DAYS_BEFORE_GAME = 0
DAYS_AFTER_GAME = 1

# --- NEW: Smart Sleep Configuration ---
SMART_SLEEP_TARGET_QPM = 85 # Target QPM (e.g., 85 to stay under 100)
SMART_SLEEP_DEFAULT_DELAY = 60 / SMART_SLEEP_TARGET_QPM # Calculated base delay
SMART_SLEEP_RATELIMIT_FLOOR = 10 # If remaining requests hit this, sleep until reset

LOG_FILE = "data_acquisition_reddit.log"
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(module)s - %(funcName)s - %(message)s',
                    handlers=[logging.FileHandler(LOG_FILE),
                              logging.StreamHandler()])

# --- Helper Functions ---
def initialize_reddit_client():
    # ... (same as before, ensure credentials are real) ...
    try:
        reddit = praw.Reddit(
            client_id=REDDIT_CLIENT_ID,
            client_secret=REDDIT_CLIENT_SECRET,
            user_agent=REDDIT_USER_AGENT,
            check_for_async=False
        )
        reddit.read_only = True
        logging.info(f"PRAW Reddit client initialized. Read-only: {reddit.read_only}")
        return reddit
    except Exception as e:
        logging.error(f"Failed to initialize PRAW Reddit client: {e}")
        return None

def smart_sleep_and_log(reddit_client, context="api_call"):
    """Uses X-Ratelimit headers to sleep smartly and logs them."""
    # Accessing the last response requires a bit of care with PRAW versions
    # For PRAW 7.x, it's usually on reddit_client._core._last_response
    last_response = getattr(getattr(reddit_client, '_core', None), '_last_response', None)
    
    if last_response and hasattr(last_response, 'headers'):
        headers = last_response.headers
        used = headers.get("x-ratelimit-used", "N/A")
        remaining = headers.get("x-ratelimit-remaining", "N/A")
        reset_in_seconds = headers.get("x-ratelimit-reset", "N/A")
        logging.debug(f"Ratelimit after {context}: Used={used}, Remaining={remaining}, ResetIn={reset_in_seconds}s")

        try:
            remaining_float = float(remaining)
            reset_in_float = float(reset_in_seconds)
            if remaining_float <= SMART_SLEEP_RATELIMIT_FLOOR:
                sleep_duration = reset_in_float + 1 # Sleep until reset + 1s buffer
                logging.warning(f"Rate limit floor hit (Remaining: {remaining_float}). Sleeping for {sleep_duration:.2f}s.")
                time.sleep(sleep_duration)
                return
        except ValueError:
            logging.debug("Could not parse ratelimit headers for smart sleep.")
            pass # Fall through to default sleep if headers can't be parsed
    
    # Default sleep if headers aren't available or not hitting floor
    time.sleep(SMART_SLEEP_DEFAULT_DELAY)


def load_player_nicknames(file_path):
    # ... (same as before, ensure your nicknames.json is correct) ...
    try:
        with open(file_path, 'r') as f:
            nickname_data = json.load(f)
        name_to_queries = {}
        for entry in nickname_data:
            canonical_name = entry.get('name')
            if not canonical_name:
                logging.warning(f"Skipping entry in nicknames file due to missing 'name': {entry}")
                continue
            nicknames = [str(n).strip() for n in entry.get('nicknames', []) if str(n).strip()] # Cleaned
            # --- MODIFICATION: Create OR-combined query string ---
            all_terms = [canonical_name.strip()] + nicknames
            # Escape quotes within terms if any, though unlikely for names
            escaped_terms = [term.replace('"', '\\"') for term in all_terms]
            or_query = " OR ".join([f'"{term}"' for term in escaped_terms]) # "Name1" OR "Nickname2"
            name_to_queries[canonical_name.lower()] = or_query
        logging.info(f"Loaded OR-combined nickname queries for {len(name_to_queries)} players.")
        return name_to_queries
    except FileNotFoundError:
        logging.error(f"Nickname file not found: {file_path}")
        return {}
    except json.JSONDecodeError:
        logging.error(f"Error decoding JSON from nickname file: {file_path}")
        return {}


def get_player_game_dates(player_name_from_list, seasons_to_consider):
    # ... (same as before) ...
    game_dates = set()
    player_file_slug = player_name_from_list.replace(' ', '_').lower()
    for season_year in seasons_to_consider:
        stats_file_path = os.path.join(PLAYER_STATS_BASE_DIR, f"season_{season_year}", f"{player_file_slug}_gamelog.csv")
        if os.path.exists(stats_file_path):
            try:
                df = pd.read_csv(stats_file_path)
                parsed_dates = pd.to_datetime(df['GAME_DATE'], format='%b %d, %Y', errors='coerce')
                valid_dates = parsed_dates.dropna().dt.date.tolist()
                game_dates.update(valid_dates)
            except Exception as e:
                logging.warning(f"Could not read/parse game dates from {stats_file_path}: {e}")
        else:
            logging.debug(f"No stats CSV for {player_name_from_list}, season {season_year} at {stats_file_path}")
    sorted_dates = sorted(list(game_dates))
    logging.info(f"Found {len(sorted_dates)} unique game dates for {player_name_from_list}")
    return sorted_dates

def fetch_reddit_mentions(reddit_client, player_name_canonical, or_combined_query, game_date, team_subreddit=None):
    collected_posts_data = []
    start_search_dt = datetime.combine(game_date, datetime.min.time()) - timedelta(days=DAYS_BEFORE_GAME)
    end_search_dt = datetime.combine(game_date, datetime.max.time()) + timedelta(days=DAYS_AFTER_GAME)

    subreddits_to_query = GENERAL_SUBREDDITS[:]
    if team_subreddit and team_subreddit not in subreddits_to_query:
        subreddits_to_query.append(team_subreddit)
    
    logging.debug(f"Effective subreddits for {player_name_canonical}: {subreddits_to_query}")

    for subreddit_name in subreddits_to_query:
        subreddit = reddit_client.subreddit(subreddit_name)
        logging.debug(f"Searching r/{subreddit_name} with OR-query for '{player_name_canonical}' around {game_date.strftime('%Y-%m-%d')}")
        # Using the OR-combined query string directly
        # Adding time_filter if PRAW supports it well with general search, otherwise manual filtering is fine
        # For now, relying on sort='new' and manual date filtering.
        try:
            # --- SINGLE SEARCH CALL PER SUBREDDIT using OR-combined query ---
            for post in subreddit.search(query=or_combined_query, sort='new', limit=POST_SEARCH_LIMIT_PER_QUERY):
                post_created_dt = datetime.fromtimestamp(post.created_utc)

                if start_search_dt <= post_created_dt <= end_search_dt:
                    # --- QUICK WIN: Only process comments for higher score posts ---
                    if post.score < MIN_POST_SCORE_FOR_DEEP_COMMENTS and FETCH_DEEP_COMMENTS:
                        logging.debug(f"Skipping deep comments for low-score post (ID: {post.id}, Score: {post.score})")
                        comments_to_fetch_limit = 0 # Fetch no comments for low score
                    elif not FETCH_DEEP_COMMENTS:
                        comments_to_fetch_limit = 0 # Top-level only mode
                    else:
                        comments_to_fetch_limit = None # Try to fetch all, then apply COMMENT_LIMIT_PER_POST

                    all_comments_text = []
                    if comments_to_fetch_limit is not None or FETCH_DEEP_COMMENTS: # Proceed if we need any comments
                        try:
                            # For FETCH_DEEP_COMMENTS = True: limit=None tries for all
                            # For FETCH_DEEP_COMMENTS = False OR low score: limit=0 for top-level only
                            post.comments.replace_more(limit= 0 if not FETCH_DEEP_COMMENTS or (post.score < MIN_POST_SCORE_FOR_DEEP_COMMENTS) else None)
                            smart_sleep_and_log(reddit_client, context=f"replace_more for post {post.id}")

                            comment_count_for_this_post = 0
                            for comment in post.comments.list():
                                if isinstance(comment, praw.models.Comment):
                                    all_comments_text.append(comment.body)
                                    comment_count_for_this_post += 1
                                    if comment_count_for_this_post >= COMMENT_LIMIT_PER_POST:
                                        break
                        except Exception as comment_e:
                            logging.error(f"Error fetching/processing comments for post {post.id}: {comment_e}")
                    
                    collected_posts_data.append({
                        'player_name_canonical': player_name_canonical,
                        'search_query_used': or_combined_query, # This is now the OR string
                        'game_date_reference': game_date.strftime('%Y-%m-%d'),
                        'subreddit': subreddit_name,
                        'post_id': post.id,
                        'post_title': post.title,
                        'post_body': post.selftext,
                        'post_score': post.score,
                        'post_num_comments_total': post.num_comments,
                        'comments_scraped_count': len(all_comments_text),
                        'post_url': "https://www.reddit.com" + post.permalink,
                        'post_created_utc': post_created_dt.strftime('%Y-%m-%d %H:%M:%S'),
                        'scraped_comments_sample': " || ".join(all_comments_text)
                    })
            # --- Smart sleep after each subreddit.search call ---
            smart_sleep_and_log(reddit_client, context=f"search r/{subreddit_name}")

        except praw.exceptions.PRAWException as pe:
            logging.error(f"PRAW API error searching r/{subreddit_name} for '{player_name_canonical}': {pe}")
            time.sleep(SMART_SLEEP_DEFAULT_DELAY * 5) # Longer pause for API errors
        except Exception as e:
            logging.error(f"General error searching r/{subreddit_name} for '{player_name_canonical}': {e}")
            time.sleep(SMART_SLEEP_DEFAULT_DELAY * 3)
            
    return collected_posts_data

# --- Main Execution ---
if __name__ == "__main__":
    os.makedirs(REDDIT_OUTPUT_DIR, exist_ok=True)
    logging.info("Starting Reddit data acquisition process (Optimized)...")

    reddit = initialize_reddit_client()
    if not reddit:
        logging.critical("Reddit client failed to initialize. Exiting.")
        exit()

    target_players_from_file = read_player_list()
    if not target_players_from_file:
        logging.error("No players found in players.txt. Exiting.")
        exit()

    player_name_to_or_queries_map = load_player_nicknames(PLAYER_NICKNAMES_FILE) # Now gets OR-queries
    if not player_name_to_or_queries_map:
        # If nicknames file is missing, create a fallback OR query with just the canonical name
        logging.warning("Nickname map is empty. Building fallback OR-queries with canonical names only.")
        player_name_to_or_queries_map = {} # Initialize an empty dict
        for name in target_players_from_file:
            # Clean the name and escape any double quotes within the name itself
            cleaned_name = name.strip().replace('"', '\\"') 
            # Construct the query string: "Player Name"
            player_name_to_or_queries_map[name.lower()] = f'"{cleaned_name}"'
    

    try:
        from newNBAstats import SEASONS_TO_FETCH as NBA_SEASONS
        logging.info(f"Imported NBA_SEASONS from newNBAstats.py: {NBA_SEASONS}")
    except ImportError:
        logging.warning("Could not import SEASONS_TO_FETCH from newNBAstats.py. Using default.")
        NBA_SEASONS = ["2022", "2023", "2024"] 

    total_players = len(target_players_from_file)
    for player_idx, player_name_in_list in enumerate(target_players_from_file):
        logging.info(f"--- Processing Player: {player_name_in_list} ({player_idx+1}/{total_players}) ---")

        # Get the OR-combined query string for the player
        or_query_for_player = player_name_to_or_queries_map.get(player_name_in_list.lower())
        if not or_query_for_player: # Should not happen if fallback is created, but as a safeguard
            logging.warning(f"No OR-query found for {player_name_in_list}. Using canonical name only.")
            temp_cleaned_name_for_fallback = player_name_in_list.strip().replace('"', '\"')
            or_query_for_player = f'"{temp_cleaned_name_for_fallback}"'
        
        logging.info(f"Using OR-combined search query for {player_name_in_list}")
        # To avoid logging massive OR strings every time:
        # logging.debug(f"OR Query: {or_query_for_player}") 

        player_team_sub = PLAYER_TEAM_SUBREDDITS.get(player_name_in_list)
        if player_team_sub:
            logging.info(f"Will also search team subreddit r/{player_team_sub}")
        
        game_dates = get_player_game_dates(player_name_in_list, NBA_SEASONS)
        if not game_dates:
            logging.warning(f"No game dates for {player_name_in_list}. Skipping Reddit search.")
            continue

        player_all_mentions_data = []
        total_game_dates = len(game_dates)
        for i, game_dt_obj in enumerate(game_dates):
            logging.info(f"Fetching mentions for {player_name_in_list} around game {game_dt_obj.strftime('%Y-%m-%d')} ({i+1}/{total_game_dates})")
            
            mentions_for_gamedate = fetch_reddit_mentions(
                reddit, 
                player_name_in_list, 
                or_query_for_player, # Pass the single OR-combined query string
                game_dt_obj,
                team_subreddit=player_team_sub
            )
            
            if mentions_for_gamedate:
                player_all_mentions_data.extend(mentions_for_gamedate)
        
        # Smart sleep after processing all game dates for a player, before starting next player
        # This is optional, as sleeps are already within the fetch_reddit_mentions
        # smart_sleep_and_log(reddit, context="end_of_player_processing")


        if player_all_mentions_data:
            df_player_mentions = pd.DataFrame(player_all_mentions_data)
            output_filename = os.path.join(REDDIT_OUTPUT_DIR, f"{player_name_in_list.replace(' ', '_').lower()}_reddit_mentions.csv")
            try:
                df_player_mentions.to_csv(output_filename, index=False, encoding='utf-8-sig') # utf-8-sig for Excel
                logging.info(f"✅ Saved {len(df_player_mentions)} Reddit mentions for {player_name_in_list} to {output_filename}")
            except Exception as e:
                logging.error(f"Error saving CSV for {player_name_in_list}: {e}")

        else:
            logging.info(f"No Reddit mentions found for {player_name_in_list} across all game dates.")

    logging.info("Reddit data acquisition process finished.") 