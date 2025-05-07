import snscrape.modules.twitter as sntwitter
import pandas as pd
import os

def fetch_tweets(player_name, max_tweets=500, since="2024-01-01", until="2024-12-31"):
    query = f'"{player_name}" since:{since} until:{until}'
    print(f"Scraping tweets for: {player_name}")
    tweets = []

    for i, tweet in enumerate(sntwitter.TwitterSearchScraper(query).get_items()):
        if i >= max_tweets:
            break
        tweets.append([tweet.date, tweet.content])

        if i % 100 == 0 and i != 0:
            print(f"{i} tweets scraped...")

    df = pd.DataFrame(tweets, columns=['Date', 'Tweet'])

    os.makedirs('data', exist_ok=True)
    file_name = f"data/{player_name.replace(' ', '_').lower()}_tweets.csv"
    df.to_csv(file_name, index=False)
    print(f"Saved {len(df)} tweets to {file_name}")

def read_player_list(file_path='players.txt'):
    with open(file_path, 'r') as f:
        players = [line.strip() for line in f if line.strip()]
    return players

if __name__ == "__main__":
    max_tweets = 1000

    since = "2024-01-01"

    until = "2024-12-31"

    player_list = read_player_list()

    print(f"Found {len(player_list)} players in players.txt")

    for player_name in player_list:
        fetch_tweets(player_name, max_tweets=max_tweets, since=since, until=until)
