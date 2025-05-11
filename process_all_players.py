#!/usr/bin/env python3
"""
process_all_players.py
----------------------
Process sentiment data for all players in the reddit_data_analyzed folder
and merge with their game stats.
"""

from pathlib import Path
import pandas as pd
import numpy as np
import re
import time
from datetime import datetime

# --------------------------- CONFIG ------------------------------------------
SEASONS = [2022, 2023, 2024]

STATS_BASE = Path("data/new/player_stats")
SENTIMENT_BASE = Path("data/new/reddit_data_analyzed")
OUT_DIR = Path("data/new/processed_data")
OUT_DIR.mkdir(parents=True, exist_ok=True)

# Skip these players if they cause issues
SKIP_PLAYERS = []
# -----------------------------------------------------------------------------


def extract_player_slug(filename):
    """Extract player slug from a filename."""
    # Extract player slug from filename like "player_name_reddit_mentions_sentiment.csv"
    match = re.match(r"([a-zA-Z0-9_-]+)_reddit_mentions_sentiment\.csv", filename)
    if match:
        return match.group(1)
    return None


def load_gamelogs(player_slug, seasons):
    """Load game logs for the specified player and seasons."""
    frames = []
    for yr in seasons:
        fp = STATS_BASE / f"season_{yr}" / f"{player_slug}_gamelog.csv"
        if fp.exists():
            print(f"  Loading game logs from {fp}")
            df = pd.read_csv(fp)
            # Convert game date to datetime with consistent format
            df["game_date"] = pd.to_datetime(df["GAME_DATE"], format="%b %d, %Y", errors="coerce")
            frames.append(df)
        else:
            print(f"  ⚠️ Missing {fp}")
    
    if not frames:
        print(f"  ❌ No game logs found for {player_slug}")
        return None
    
    combined = pd.concat(frames, ignore_index=True).sort_values("game_date")
    print(f"  ✓ Loaded {len(combined)} games across {len(frames)} seasons")
    return combined


def load_sentiment(sentiment_file):
    """Load and preprocess Reddit sentiment data from file."""
    print(f"  Loading sentiment data from {sentiment_file}")
    if not sentiment_file.exists():
        print(f"  ❌ Sentiment file not found: {sentiment_file}")
        return None
    
    try:
        df = pd.read_csv(sentiment_file)
        
        # Ensure post_created_utc is in datetime format
        df["post_created_utc"] = pd.to_datetime(df["post_created_utc"], errors='coerce')
        
        # Drop rows with invalid dates
        invalid_dates = df["post_created_utc"].isna().sum()
        if invalid_dates > 0:
            print(f"  ⚠️ Dropping {invalid_dates} rows with invalid dates")
            df = df.dropna(subset=["post_created_utc"])
        
        # Calculate compound sentiment as average of title, body, and comments
        df["compound_avg"] = df[["title_compound", 
                                "body_compound",
                                "comments_compound"]].mean(axis=1)
        
        # Floor date to day for matching
        df["post_date"] = df["post_created_utc"].dt.floor("D")
        
        # Handle duplicate post_ids
        duplicates = df[df.duplicated(subset=['post_id'], keep=False)]
        if len(duplicates) > 0:
            print(f"  ⚠️ Found {len(duplicates)} entries with duplicate post_ids")
            # Remove duplicates keeping the first occurrence of each post_id
            df = df.drop_duplicates(subset=['post_id'], keep='first')
        
        # Check for NaN sentiment values
        nan_count = df["compound_avg"].isna().sum()
        if nan_count > 0:
            print(f"  ⚠️ Found {nan_count} NaN sentiment values, dropping them")
            df = df.dropna(subset=["compound_avg"])
        
        print(f"  ✓ Loaded {len(df)} unique posts across {df['post_date'].nunique()} dates")
        
        return df[["post_date", "post_created_utc", "compound_avg", "post_id"]].sort_values("post_date")
    
    except Exception as e:
        print(f"  ❌ Error loading sentiment data: {e}")
        return None


def snap_posts(posts, games):
    """Match posts to their nearest game dates."""
    if posts is None or games is None:
        return None
    
    try:
        # Ensure both dataframes have datetime indices for proper merging
        posts_sorted = posts.sort_values("post_date").copy()
        games_sorted = games[["game_date"]].sort_values("game_date").copy()
        
        print(f"  Matching {len(posts_sorted)} posts to {len(games_sorted)} games")
        
        # Use merge_asof to match each post to its nearest game date
        snapped = pd.merge_asof(
            posts_sorted,
            games_sorted,
            left_on="post_date",
            right_on="game_date",
            direction="nearest",  # Use nearest match regardless of before/after
            tolerance=None,       # No date tolerance - match everything
        )
        
        # Compute absolute gap in days between post and game
        snapped["delta_days"] = (
            (snapped["post_date"] - snapped["game_date"]).dt.total_seconds() / (86400)
        ).abs()
        
        # Count posts per game date
        posts_per_game = snapped.groupby("game_date").size()
        
        print(f"  ✓ Matched posts to {snapped['game_date'].nunique()} unique game dates")
        print(f"  ✓ Games with posts: 1 post: {(posts_per_game == 1).sum()}, " +
              f"2 posts: {(posts_per_game == 2).sum()}, " +
              f"3+ posts: {(posts_per_game >= 3).sum()}")
        
        return snapped
    
    except Exception as e:
        print(f"  ❌ Error matching posts to games: {e}")
        return None


def aggregate_sentiment(snapped):
    """Aggregate sentiment data by game date."""
    if snapped is None:
        return None
    
    try:
        # Calculate aggregate stats per game date
        agg_data = (
            snapped.groupby("game_date")
                   .agg(
                       mean_sentiment=("compound_avg", "mean"),
                       min_sentiment=("compound_avg", "min"),
                       max_sentiment=("compound_avg", "max"),
                       pos_share=("compound_avg", lambda x: (x > 0).mean()),
                       neg_share=("compound_avg", lambda x: (x < 0).mean()),
                       post_count=("compound_avg", "size"),
                       avg_delta_days=("delta_days", "mean"),
                       min_delta_days=("delta_days", "min"),
                       max_delta_days=("delta_days", "max")
                   )
                   .reset_index()
        )
        
        print(f"  ✓ Aggregated sentiment for {len(agg_data)} game dates")
        return agg_data
    
    except Exception as e:
        print(f"  ❌ Error aggregating sentiment: {e}")
        return None


def merge_with_stats(stats, sentiment):
    """Merge game stats with sentiment data."""
    if stats is None or sentiment is None:
        return None
    
    try:
        # Use left merge to ensure we keep all games, then fill missing sentiment with 0
        final = (
            stats.merge(sentiment, on="game_date", how="left")
                 .fillna({
                     "mean_sentiment": 0,
                     "min_sentiment": 0,
                     "max_sentiment": 0,
                     "pos_share": 0,
                     "neg_share": 0,
                     "post_count": 0,
                     "avg_delta_days": np.nan,
                     "min_delta_days": np.nan,
                     "max_delta_days": np.nan
                 })
        )
        
        # Add additional columns that might be useful for analysis
        final["has_sentiment_data"] = final["post_count"] > 0
        
        # Count games with sentiment data
        games_with_sentiment = final["has_sentiment_data"].sum()
        print(f"  ✓ Merged data: {games_with_sentiment} of {len(final)} games " +
              f"({games_with_sentiment/len(final)*100:.1f}%) have sentiment data")
        
        return final
    
    except Exception as e:
        print(f"  ❌ Error merging stats with sentiment: {e}")
        return None


def process_player(player_slug):
    """Process data for a single player."""
    print(f"\n{'='*30} Processing {player_slug} {'='*30}")
    
    # Set paths
    sentiment_file = SENTIMENT_BASE / f"{player_slug}_reddit_mentions_sentiment.csv"
    out_file = OUT_DIR / f"{player_slug}_stats_sentiment_{SEASONS[0]}_{SEASONS[-1]}.csv"
    
    # Load game logs
    stats = load_gamelogs(player_slug, SEASONS)
    if stats is None:
        return False
    
    # Load sentiment data
    posts = load_sentiment(sentiment_file)
    if posts is None:
        return False
    
    # Match posts to games
    snapped = snap_posts(posts, stats)
    if snapped is None:
        return False
    
    # Aggregate sentiment by game date
    daily_sent = aggregate_sentiment(snapped)
    if daily_sent is None:
        return False
    
    # Merge with game stats
    final = merge_with_stats(stats, daily_sent)
    if final is None:
        return False
    
    # Save to CSV
    try:
        final.to_csv(out_file, index=False)
        print(f"  ✅ Saved {len(final)} rows to {out_file}")
        return True
    except Exception as e:
        print(f"  ❌ Error saving to CSV: {e}")
        try:
            # Try alternative location if permission issues
            alt_file = Path(f"{player_slug}_merged_data.csv")
            final.to_csv(alt_file, index=False)
            print(f"  ✅ Saved to alternative location: {alt_file}")
            return True
        except Exception as e2:
            print(f"  ❌ Error saving to alternative location: {e2}")
            return False


def main():
    """Process sentiment data for all players."""
    start_time = time.time()
    print(f"Starting sentiment processing for all players at {datetime.now()}")
    print(f"Seasons: {SEASONS}")
    print(f"Output directory: {OUT_DIR}")
    print("-" * 80)
    
    # Get list of sentiment files
    sentiment_files = list(SENTIMENT_BASE.glob("*_reddit_mentions_sentiment.csv"))
    print(f"Found {len(sentiment_files)} sentiment files to process")
    
    # Process each player
    success_count = 0
    for sentiment_file in sentiment_files:
        player_slug = extract_player_slug(sentiment_file.name)
        if player_slug is None:
            print(f"Skipping {sentiment_file.name}: couldn't extract player slug")
            continue
        
        if player_slug in SKIP_PLAYERS:
            print(f"Skipping {player_slug} (in skip list)")
            continue
        
        success = process_player(player_slug)
        if success:
            success_count += 1
    
    # Print summary
    elapsed_time = time.time() - start_time
    print("\n" + "=" * 80)
    print(f"Processing complete: {success_count}/{len(sentiment_files)} players processed successfully")
    print(f"Total time: {elapsed_time:.1f} seconds")
    print("=" * 80)


if __name__ == "__main__":
    main() 