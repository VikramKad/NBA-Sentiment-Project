import pandas as pd
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import os
import logging

# Ensure VADER lexicon is available
try:
    nltk.data.find('sentiment/vader_lexicon.zip')
except nltk.downloader.DownloadError:
    nltk.download('vader_lexicon')

# Initialize the Sentiment Intensity Analyzer
sia = SentimentIntensityAnalyzer()

# --- Configuration ---
# Base directory where your 'new' folder containing 'reddit_data' and 'player_stats' is
BASE_DATA_DIR = 'data/new/'
REDDIT_DATA_DIR = os.path.join(BASE_DATA_DIR, 'reddit_data')
PLAYER_STATS_DIR = os.path.join(BASE_DATA_DIR, 'player_stats')
OUTPUT_DIR = os.path.join(BASE_DATA_DIR, 'processed_data')

# Ensure output directory exists
os.makedirs(OUTPUT_DIR, exist_ok=True)

# List of player file slugs
PLAYER_SLUGS = [
    "anthony_edwards",
    "donovan_mitchell",
    "giannis_antetokounmpo",
    "jalen_brunson",
    "lebron_james",
    "luka_doncic",
    "shai_gilgeous_alexander",
    "stephen_curry"
]

# Season for game logs
TARGET_SEASON_FOR_STATS = "2023"  # Corresponds to the 2023-24 NBA season

def process_player_data(player_slug, target_season):
    """
    Loads Reddit data and player stats, performs sentiment analysis,
    aggregates sentiment, merges data, and saves the result.
    """
    logging.info(f"--- Processing player: {player_slug} for season {target_season} ---")

    # --- 1. Load Reddit Data ---
    reddit_file_path = os.path.join(REDDIT_DATA_DIR, f"{player_slug}_reddit_mentions.csv")
    if not os.path.exists(reddit_file_path):
        logging.error(f"Reddit mentions file not found for {player_slug}: {reddit_file_path}")
        return None
    
    try:
        reddit_df = pd.read_csv(reddit_file_path)
        logging.info(f"Loaded {len(reddit_df)} Reddit mentions for {player_slug}.")
    except Exception as e:
        logging.error(f"Error loading Reddit data for {player_slug}: {e}")
        return None

    if reddit_df.empty:
        logging.warning(f"Reddit data for {player_slug} is empty. Skipping further processing for this player.")
        return None

    # --- 2. Sentiment Scoring ---
    # Concatenate title, body, and comments for sentiment analysis
    text_columns = ['post_title', 'post_body', 'scraped_comments_sample']
    for col in text_columns:
        if col not in reddit_df.columns:
            logging.warning(f"Column '{col}' not found in Reddit data for {player_slug}. Will use empty string.")
            reddit_df[col] = ""

    reddit_df['combined_text'] = reddit_df[text_columns].fillna('').agg(' '.join, axis=1)
    
    # Apply VADER sentiment analysis
    try:
        reddit_df['compound_sentiment'] = reddit_df['combined_text'].apply(
            lambda txt: sia.polarity_scores(str(txt))['compound']
        )
        logging.info(f"Calculated compound sentiment for {player_slug}.")
    except Exception as e:
        logging.error(f"Error during sentiment scoring for {player_slug}: {e}")
        reddit_df['compound_sentiment'] = 0.0  # Fallback to neutral

    # --- 3. Aggregate Sentiment per game_date_reference ---
    if 'game_date_reference' not in reddit_df.columns:
        logging.error(f"'game_date_reference' column missing in Reddit data for {player_slug}.")
        return None

    daily_sentiment = reddit_df.groupby('game_date_reference').agg(
        mean_sentiment=('compound_sentiment', 'mean'),
        positive_sentiment_ratio=('compound_sentiment', lambda x: (x > 0.05).mean()),
        negative_sentiment_ratio=('compound_sentiment', lambda x: (x < -0.05).mean()),
        mention_count=('compound_sentiment', 'size')
    ).reset_index()
    logging.info(f"Aggregated daily sentiment for {player_slug}.")

    # --- 4. Load Player Game Stats ---
    stats_file_path = os.path.join(PLAYER_STATS_DIR, f"season_{target_season}", f"{player_slug}_gamelog.csv")
    if not os.path.exists(stats_file_path):
        logging.error(f"Player stats file not found for {player_slug}, season {target_season}: {stats_file_path}")
        return None 
        
    try:
        stats_df = pd.read_csv(stats_file_path)
        logging.info(f"Loaded {len(stats_df)} game log entries for {player_slug}, season {target_season}.")
    except Exception as e:
        logging.error(f"Error loading player stats for {player_slug}, season {target_season}: {e}")
        return None

    if 'GAME_DATE' not in stats_df.columns:
        logging.error(f"'GAME_DATE' column missing in stats data for {player_slug}, season {target_season}.")
        return None

    # --- 5. Merge Sentiment with Stats ---
    # Convert GAME_DATE in stats_df to 'YYYY-MM-DD' format
    try:
        stats_df['game_date_reference'] = pd.to_datetime(stats_df['GAME_DATE'], format='%b %d, %Y').dt.strftime('%Y-%m-%d')
    except ValueError as e:
        logging.error(f"Error converting GAME_DATE format for {player_slug}: {e}. Trying general date parsing.")
        try:
            stats_df['game_date_reference'] = pd.to_datetime(stats_df['GAME_DATE']).dt.strftime('%Y-%m-%d')
        except Exception as e_gen:
            logging.error(f"General date parsing also failed for {player_slug}: {e_gen}. Cannot proceed with merge.")
            return None

    # Merge stats with daily sentiment
    merged_df = pd.merge(stats_df, daily_sentiment, on='game_date_reference', how='left')
    
    # Fill NaN values for sentiment columns (for game days with no Reddit mentions)
    sentiment_cols_to_fill = ['mean_sentiment', 'positive_sentiment_ratio', 'negative_sentiment_ratio', 'mention_count']
    fill_values = {
        'mean_sentiment': 0.0,  # Neutral sentiment
        'positive_sentiment_ratio': 0.0,
        'negative_sentiment_ratio': 0.0,
        'mention_count': 0      # Zero mentions
    }
    for col in sentiment_cols_to_fill:
        if col in merged_df.columns:
            merged_df[col] = merged_df[col].fillna(fill_values.get(col, 0))
        else:
            merged_df[col] = fill_values.get(col, 0)

    logging.info(f"Merged stats and sentiment for {player_slug}. Resulting shape: {merged_df.shape}")

    # --- 6. Save Processed Data ---
    output_file_path = os.path.join(OUTPUT_DIR, f"{player_slug}_stats_plus_sentiment_{target_season}.csv")
    try:
        merged_df.to_csv(output_file_path, index=False, encoding='utf-8-sig')
        logging.info(f"✅ Successfully saved processed data to {output_file_path}")
    except Exception as e:
        logging.error(f"Error saving processed data for {player_slug}: {e}")

    return merged_df


# --- Main Loop to Process All Players ---
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(filename)s - %(funcName)s - %(message)s',
                        handlers=[logging.FileHandler("data_processing.log"),
                                  logging.StreamHandler()])
    
    all_players_merged_data = {}

    for slug in PLAYER_SLUGS:
        processed_df = process_player_data(slug, TARGET_SEASON_FOR_STATS)
        if processed_df is not None:
            all_players_merged_data[slug] = processed_df
            logging.info(f"Finished processing for {slug}.")
        else:
            logging.warning(f"Processing failed or resulted in no data for {slug}.")
            
    logging.info("--- All players processed. ---") 