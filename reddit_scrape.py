from datetime import datetime
import praw
import pandas as pd
import os
from readPL import read_player_list

# Set up Reddit API
reddit = praw.Reddit(
    client_id='E2MfNNRsw6F5YfvVoc8Jtg',
    client_secret='Ns86Hi0A_jcWH2TwzWE5JBGsXZKBLw',
    user_agent='NBA Sent Gatherer by /u/Long-Place-9308'
)

def fetch_posts_and_comments(subreddit_name, query, post_limit, comment_limit, start_date, end_date):
    subreddit = reddit.subreddit(subreddit_name)
    posts_data = []

    print(f"Searching '{query}' in r/{subreddit_name} from {start_date.date()} to {end_date.date()}...")

    for post in subreddit.search(query, limit=post_limit):
        post_date = datetime.fromtimestamp(post.created_utc)

        if start_date and post_date < start_date:
            continue
        if end_date and post_date > end_date:
            continue

        post.comments.replace_more(limit=0)
        comments = [comment.body for comment in post.comments[:comment_limit]]

        posts_data.append({
            'Game Date': start_date.strftime('%Y-%m-%d'),  # Adds which game date window this search is for
            'Post Title': post.title,
            'Post Body': post.selftext,
            'Comments': ' || '.join(comments),
            'Reddit Post Date': post_date.strftime('%Y-%m-%d')
        })

    df = pd.DataFrame(posts_data)
    return df

def get_game_dates(player_name):
    file_name = f"data/{player_name.replace(' ', '_').lower()}_stats.csv"
    if not os.path.exists(file_name):
        print(f"No stats CSV found for {player_name}")
        return []
    df = pd.read_csv(file_name)
    # Parse date safely (explicit format)
    return pd.to_datetime(df['GAME_DATE'], format='%b %d, %Y').tolist()

if __name__ == "__main__":
    player_list = read_player_list()

    for player_name in player_list:
        print(f"\nGathering Reddit posts for {player_name}...")
        game_dates = get_game_dates(player_name)
        all_posts = pd.DataFrame()  # Empty DataFrame to collect everything

        for game_date in game_dates:
            start = game_date
            end = game_date + pd.Timedelta(days=1)

            df_posts = fetch_posts_and_comments(
                subreddit_name='nba',
                query=player_name,
                post_limit=100,
                comment_limit=20,
                start_date=start,
                end_date=end
            )

            all_posts = pd.concat([all_posts, df_posts], ignore_index=True)

        # Save one combined CSV for the player
        os.makedirs('data', exist_ok=True)
        file_name = f"data/{player_name.replace(' ', '_').lower()}_reddit_posts.csv"
        all_posts.to_csv(file_name, index=False)
        print(f"✅ Saved {len(all_posts)} total posts to {file_name}")
