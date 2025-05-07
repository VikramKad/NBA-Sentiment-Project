import praw
import pandas as pd
import os
from readPL import read_player_list

reddit = praw.Reddit(
    client_id='E2MfNNRsw6F5YfvVoc8Jtg',
    client_secret='Ns86Hi0A_jcWH2TwzWE5JBGsXZKBLw',
    user_agent='NBA Sent Gatherer by /u/Long-Place-9308'
)

def fetch_posts_and_comments(subreddit_name, query, post_limit, comment_limit):
    subreddit = reddit.subreddit(subreddit_name)
    posts_data = []

    print(f"Searching '{query}' in r/{subreddit_name}...")

    for post in subreddit.search(query, limit=post_limit):
        post.comments.replace_more(limit=0) 
        comments = [comment.body for comment in post.comments[:comment_limit]]

        posts_data.append({
            'Post Title': post.title,
            'Post Body': post.selftext,
            'Comments': comments
        })

    df = pd.DataFrame(posts_data)
    os.makedirs('data', exist_ok=True)
    file_name = f"data/{query.replace(' ', '_').lower()}_reddit_posts.csv"
    df.to_csv(file_name, index=False)
    print(f"Saved {len(df)} posts to {file_name}")

if __name__ == "__main__":

    player_list = read_player_list()
    
    for player_name in player_list:
        #print (player_name)
        fetch_posts_and_comments(subreddit_name= 'nba', query = player_name, post_limit=50, comment_limit=50)



    # fetch_posts_and_comments(
    #     subreddit_name='nba',
    #     query='LeBron James',
    #     post_limit=20,
    #     comment_limit=20
    # )
