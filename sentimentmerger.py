#!/usr/bin/env python3
"""
sentimentmerger.py
-----------------
• Every Reddit post is matched to its nearest game (no date tolerance cut-off)
• Stores absolute gap in days between post and game (delta_days)
• Aggregates sentiment & gap stats per game date
"""

from pathlib import Path
import pandas as pd
import numpy as np
from datetime import datetime

# --------------------------- CONFIG ------------------------------------------
PLAYER_SLUG   = "anthony_edwards"
SEASONS       = [2022, 2023, 2024]

STATS_BASE    = Path("data/new/player_stats")
SENTIMENT_CSV = Path("data/new/reddit_data_analyzed") / (
    f"{PLAYER_SLUG}_reddit_mentions_sentiment.csv"
)

OUT_DIR       = Path("data/new/processed_data")
OUT_DIR.mkdir(parents=True, exist_ok=True)
OUT_CSV       = OUT_DIR / (
    f"{PLAYER_SLUG}_merged_sentiment_{SEASONS[0]}_{SEASONS[-1]}.csv"
)
# -----------------------------------------------------------------------------


def load_gamelogs() -> pd.DataFrame:
    """Load game logs for all specified seasons and combine them."""
    frames = []
    for yr in SEASONS:
        fp = STATS_BASE / f"season_{yr}" / f"{PLAYER_SLUG}_gamelog.csv"
        if fp.exists():
            print(f"Loading game logs from {fp}")
            df = pd.read_csv(fp)
            # Convert game date to datetime with consistent format
            df["game_date"] = pd.to_datetime(df["GAME_DATE"], format="%b %d, %Y")
            frames.append(df)
        else:
            print(f"⚠️  Missing {fp}")
    
    if not frames:
        raise FileNotFoundError("No game‑log CSVs found.")
    
    return pd.concat(frames, ignore_index=True).sort_values("game_date")


def load_sentiment() -> pd.DataFrame:
    """Load and preprocess Reddit sentiment data."""
    print(f"Loading sentiment data from {SENTIMENT_CSV}")
    if not SENTIMENT_CSV.exists():
        raise FileNotFoundError(f"Sentiment CSV not found: {SENTIMENT_CSV}")
    
    df = pd.read_csv(SENTIMENT_CSV)
    
    # Ensure post_created_utc is in datetime format
    df["post_created_utc"] = pd.to_datetime(df["post_created_utc"])
    
    # Calculate compound sentiment as average of title, body, and comments
    df["compound_avg"] = df[["title_compound", 
                              "body_compound",
                              "comments_compound"]].mean(axis=1)
    
    # Floor date to day for matching
    df["post_date"] = df["post_created_utc"].dt.floor("D")
    
    print(f"Loaded {len(df)} Reddit posts with sentiment data")
    print(f"Number of unique posts (by post_id): {df['post_id'].nunique()}")
    print(f"Number of unique dates: {df['post_date'].nunique()}")
    
    # Handle duplicate post_ids by keeping only the first occurrence
    duplicates = df[df.duplicated(subset=['post_id'], keep=False)]
    if len(duplicates) > 0:
        print(f"Found {len(duplicates)} entries with duplicate post_ids")
        print(f"Duplicates by post_id:")
        for post_id, group in duplicates.groupby('post_id'):
            print(f"  Post {post_id}: {len(group)} occurrences")
        
        # Remove duplicates keeping the first occurrence of each post_id
        df = df.drop_duplicates(subset=['post_id'], keep='first')
        print(f"After removing duplicates: {len(df)} unique posts")
    
    # Debug: check for NaN or invalid values in sentiment
    nan_count = df["compound_avg"].isna().sum()
    if nan_count > 0:
        print(f"Warning: Found {nan_count} NaN values in compound_avg")
        # Fill NaN with 0 or remove rows
        df = df.dropna(subset=["compound_avg"])
        
    # Debug: Check sentiment distribution
    zero_sentiment = (df["compound_avg"] == 0).sum()
    if zero_sentiment > 0:
        print(f"Note: {zero_sentiment} posts have exactly zero sentiment")
    
    # Show date range of posts
    min_date = df["post_date"].min()
    max_date = df["post_date"].max()
    print(f"Post date range: {min_date.date()} to {max_date.date()}")
    
    return df[["post_date", "post_created_utc", "compound_avg"]].sort_values("post_date")


def snap_posts(posts: pd.DataFrame, games: pd.DataFrame) -> pd.DataFrame:
    """
    Match EVERY Reddit post to its nearest game date (no tolerance).
    Calculates the absolute days difference between post and game.
    """
    # Ensure both dataframes have datetime indices for proper merging
    posts_sorted = posts.sort_values("post_date").copy()
    games_sorted = games[["game_date"]].sort_values("game_date").copy()
    
    print(f"Snapping {len(posts_sorted)} posts to {len(games_sorted)} games")
    
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
    
    # Debug: check if any posts weren't matched
    if len(snapped) < len(posts_sorted):
        print(f"⚠️ {len(posts_sorted) - len(snapped)} posts couldn't be matched to games")
    
    # Debug: check distribution of delta days
    print(f"Post-game day gaps statistics:")
    print(f"  Min gap: {snapped['delta_days'].min():.1f} days")
    print(f"  Max gap: {snapped['delta_days'].max():.1f} days")
    print(f"  Avg gap: {snapped['delta_days'].mean():.1f} days")
    print(f"  Median gap: {snapped['delta_days'].median():.1f} days")
    
    # Count the number of unique game dates after matching
    unique_game_dates = snapped["game_date"].nunique()
    print(f"Posts matched to {unique_game_dates} unique game dates")
    
    # Show the distribution of posts per game date
    posts_per_game = snapped.groupby("game_date").size()
    print(f"Posts per game date distribution:")
    print(f"  1 post: {(posts_per_game == 1).sum()} games")
    print(f"  2 posts: {(posts_per_game == 2).sum()} games")
    print(f"  3+ posts: {(posts_per_game >= 3).sum()} games")
    
    # Show some examples of games with multiple posts
    if (posts_per_game > 1).any():
        print("\nExample games with multiple posts:")
        for game_date, count in posts_per_game.nlargest(3).items():
            game_posts = snapped[snapped["game_date"] == game_date]
            print(f"  Game {game_date.date()}: {count} posts")
            for i, (_, post) in enumerate(game_posts.iterrows(), 1):
                print(f"    Post {i}: {post['post_date'].date()}, Gap: {post['delta_days']:.1f} days")
    
    return snapped


def aggregate(snapped: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate sentiment data by game date, including statistics about
    post-game time gaps.
    """
    unique_game_dates = snapped["game_date"].nunique()
    print(f"Aggregating sentiment for {unique_game_dates} game dates")
    
    # Check for zero sentiment values in the snapped data
    zero_sentiment_posts = (snapped["compound_avg"] == 0).sum()
    if zero_sentiment_posts > 0:
        print(f"  Warning: {zero_sentiment_posts} posts have exactly zero sentiment")
    
    # Get count of posts by game date
    post_counts = snapped.groupby("game_date").size()
    print(f"  Posts per game distribution:")
    print(f"    Min posts: {post_counts.min()}")
    print(f"    Max posts: {post_counts.max()}")
    print(f"    Avg posts: {post_counts.mean():.1f}")
    
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
    
    # Check for games with zero sentiment
    zero_sentiment_games = (agg_data["mean_sentiment"] == 0).sum()
    if zero_sentiment_games > 0:
        print(f"  Found {zero_sentiment_games} games with mean sentiment = 0")
    
    # Debug: show some statistics about the aggregated data
    print(f"  Sentiment statistics across {len(agg_data)} game dates:")
    print(f"    Avg sentiment: {agg_data['mean_sentiment'].mean():.4f}")
    print(f"    Min sentiment: {agg_data['mean_sentiment'].min():.4f}")
    print(f"    Max sentiment: {agg_data['mean_sentiment'].max():.4f}")
    print(f"    Avg post count per game: {agg_data['post_count'].mean():.1f}")
    
    return agg_data


def main() -> None:
    """Main function to process data and generate output file."""
    print("=" * 80)
    print(f"Starting sentiment merger for {PLAYER_SLUG} (Seasons: {SEASONS})")
    print("-" * 80)
    
    print("▶ Loading game logs...")
    stats = load_gamelogs()
    print(f"  Loaded {len(stats)} games across {len(SEASONS)} seasons")
    
    print("\n▶ Loading sentiment data...")
    posts = load_sentiment()
    
    print("\n▶ Snapping posts to nearest game (no tolerance)...")
    snapped = snap_posts(posts, stats)
    
    print("\n▶ Aggregating sentiment...")
    daily_sent = aggregate(snapped)
    
    print("\n▶ Merging with game stats...")
    # Use outer merge to ensure we keep all games, then fill missing sentiment with 0
    final = (
        stats.merge(daily_sent, on="game_date", how="left")
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
    
    # Check if we have any non-zero sentiment values
    non_zero_sentiment = (final["mean_sentiment"] != 0).sum()
    print(f"  Found {non_zero_sentiment} games with non-zero sentiment values")
    
    # Debug: examine some sample data
    if non_zero_sentiment > 0:
        print("\nSample of games with sentiment data (first 5):")
        sample = final[final["has_sentiment_data"]].head(5)
        for _, row in sample.iterrows():
            print(f"  Game {row['GAME_DATE']}: Sentiment={row['mean_sentiment']:.4f}, Posts={int(row['post_count'])}")
    
    # Print detailed sample of the first 5 rows
    print("\n▶ Detailed data sample (first 5 rows):")
    for i, row in final.head(5).iterrows():
        print(f"\nRow {i+1}:")
        print(f"  Game Date: {row['game_date']}")
        print(f"  Game: {row['MATCHUP']}, Result: {row['WL']}")
        print(f"  Stats: {row['PTS']} pts, {row['REB']} reb, {row['AST']} ast")
        print(f"  Sentiment:")
        print(f"    Mean: {row['mean_sentiment']:.4f}")
        print(f"    Post Count: {int(row['post_count'])}")
        print(f"    Pos Share: {row['pos_share']:.2f}")
    
    # Try writing to current directory if permission issues
    try:
        # Save the output
        OUT_CSV.parent.mkdir(exist_ok=True, parents=True)
        final.to_csv(OUT_CSV, index=False)
        print(f"\n✅ Saved {len(final)} rows to {OUT_CSV}")
    except (PermissionError, OSError) as e:
        # Alternative: save to current directory
        alt_output = Path(f"{PLAYER_SLUG}_merged_data.csv")
        print(f"\n⚠️  Could not write to {OUT_CSV}: {e}")
        print(f"  Trying alternative location: {alt_output}")
        final.to_csv(alt_output, index=False)
        print(f"✅ Saved {len(final)} rows to {alt_output}")
    
    # Show distribution of games with sentiment data
    games_with_sentiment = final["has_sentiment_data"].sum()
    print(f"  {games_with_sentiment} out of {len(final)} games ({games_with_sentiment/len(final)*100:.1f}%) have associated sentiment data")
    
    print("=" * 80)


if __name__ == "__main__":
    main()
