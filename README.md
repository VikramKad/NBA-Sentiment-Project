# Hype or Insight? Exploring the Link Between Reddit Reactions and NBA Performance

**Authors:**
*   Vikram Kadyan (Rutgers University, vk414@scarletmail.rutgers.edu)
*   Oways Jaffer (Rutgers University, omj9@scarletmail.rutgers.edu)

**Course Project for CS 439: Data Science, Rutgers University (NeurIPS 2024 Style Submission)**

## 1. Project Overview

This project investigates the intricate relationship between NBA fan sentiment, as expressed on Reddit, and players' in-game performance statistics. Utilizing unsupervised learning techniques, we explore and characterize the connections between key performance metrics (points, rebounds, assists, etc.) and sentiment scores derived from Reddit posts and comments.

The central research question is: *To what extent does online fan sentiment align with, and potentially reflect underlying patterns in, present player performance, and are there discernible associations that might suggest how sentiment co-evolves with performance narratives over time?*

We focused on a select group of prominent NBA stars known for consistent media attention, ensuring a rich dataset for sentiment analysis. The project involves:
1.  Collecting game-by-game performance data.
2.  Scraping and analyzing sentiment from Reddit (r/NBA) discussions related to these players around their game dates.
3.  Merging these datasets to create a comprehensive view.
4.  Employing unsupervised methods like data visualization, correlation analysis, and potentially clustering to uncover patterns and characteristic game-sentiment profiles.

This work draws on concepts from sports analytics, natural language processing (NLP), and unsupervised machine learning.

## 2. Motivation

The intersection of sports analytics and social media offers a unique lens into fan psychology and public opinion formation. Millions of fans daily discuss NBA players and games, creating a vast, untapped resource for understanding how sentiment reflects or diverges from objective performance. This project is driven by:
*   Curiosity about whether online sentiment mirrors, anticipates, or reacts to on-court achievements.
*   The desire to understand the pressures on professional athletes, especially in an era of heightened awareness around mental health.
*   An interest in combining NLP and unsupervised learning in the culturally relevant context of NBA basketball.
*   The potential value of such analysis for sports media, fantasy sports, gambling industries, and sports psychology.

## 3. Methodology

### 3.1 Dataset Description and Data Preparation

Our dataset integrates two primary sources:

*   **NBA Player Performance Statistics:**
    *   **Source:** Official NBA API (via `nba_stats` Python library).
    *   **Content:** Game-by-game box scores for selected players.
    *   **Metrics Focused On:** Points, assists, rebounds, steals, blocks, plus-minus, field goal percentage (FG\_PCT), and game outcome (Win/Loss).
    *   **Seasons:** 2022-2023, 2023-2024, and 2024-2025 NBA regular seasons.
    *   **Players Studied:** LeBron James, Stephen Curry, Jalen Brunson, Giannis Antetokounmpo, Luka Doncic, Shai Gilgeous-Alexander, Anthony Edwards, and Donovan Mitchell.

*   **Reddit Sentiment Data:**
    *   **Source:** r/NBA subreddit.
    *   **Collection:** Python Reddit API Wrapper (PRAW) to scrape posts and comments mentioning the selected players (by name or nickname) around their game dates.
    *   **Sentiment Analysis:** VADER (Valence Aware Dictionary and sEntiment Reasoner) from NLTK was used to calculate positive, neutral, negative, and compound sentiment scores for post titles, bodies, and a sample of comments.
    *   **Aggregation:** These scores were processed to create an average sentiment representation for each game.

The final dataset merges a player's game statistics with the aggregated fan sentiment from Reddit posts closest to that game, including a `delta_days` metric indicating the time difference between the post and the game.

### 3.2 Modeling Approach / Analysis (Unsupervised)

We conducted an exploratory data analysis (EDA) to:
1.  **Visualize Univariate Distributions:** Examined distributions of key performance and sentiment metrics.
2.  **Correlation Analysis:** Calculated and visualized correlation matrices to identify linear relationships between performance statistics and sentiment scores.
3.  **Bivariate Analysis:** Created scatter plots and box plots for key pairs of variables (e.g., PTS vs. mean\_sentiment, WIN vs. mean\_sentiment).
4.  **(Implied by Notebook)** **Clustering:** Applied K-Means clustering (preceded by PCA for dimensionality reduction) to identify distinct game-sentiment profiles based on a combination of performance and sentiment features.

The evaluation was based on the consistency and clarity of observed patterns across players, aiming to determine if sentiment metrics showed meaningful variation in response to game outcomes and player statistics.

## 4. Tech Stack

*   **Python 3.x**
*   **Pandas:** Data manipulation and analysis.
*   **NumPy:** Numerical operations.
*   **Matplotlib & Seaborn:** Data visualization.
*   **NLTK (VADER):** Sentiment analysis.
*   **PRAW (Python Reddit API Wrapper):** Reddit data scraping.
*   **Scikit-learn:** StandardScaler, PCA, KMeans for clustering.
*   **Jupyter Notebook:** For analysis and presentation.

## 5. Project Structure

The repository is organized as follows (see `tree.txt` for a detailed layout):

*   `NBA-Sentiment-Project/`
    *   `.python-version`
    *   `actual final v2.ipynb` (Primary analysis notebook)
    *   `All other files/`
        *   `anthony_edwards_merged_data.csv` (Example processed output)
        *   `anthony_edwards_merged_sentiment_2022_2024.csv` (Example processed output)
        *   `data_processing.log`
        *   `fetchNBA_stats.py` (Likely for NBA stats data acquisition)
        *   `readPL.py`
        *   `reddit_scrape.py` (Likely for Reddit data scraping)
        *   `make tree.py`
    *   `data/`
        *   `broken or old/`
            *   `lebron_james_reddit_mentions.csv`
        *   `csv/` (Original raw player-specific Reddit posts and potentially older stats)
            *   `anthony_edwards_reddit_posts.csv`
            *   `anthony_edwards_stats.csv`
            *   `... (files for other players) ...`
        *   `json/`
            *   `nicknames.json`
        *   `logs/` (Logs from data acquisition)
            *   `data_acquisition_nba.log`
            *   `data_acquisition_reddit.log`
            *   `data_acquisition_reddit.txt`
        *   `new/`
            *   `player_stats/` (Raw game logs per player)
                *   `player_id_cache.json`
                *   `player_nicknames.json.json`
                *   `season_2022/`
                    *   `anthony_edwards_gamelog.csv`
                    *   `... (gamelogs for other players and seasons 2023, 2024) ...`
            *   `processed_data/` (**Final merged datasets per player: Stats + Sentiment**)
                *   `anthony_edwards_stats_sentiment_2022_2024.csv`
                *   `donovan_mitchell_stats_sentiment_2022_2024.csv`
                *   `... (files for other players) ...`
            *   `reddit_data/` (Raw Reddit mentions CSVs per player)
                *   `anthony_edwards_reddit_mentions.csv`
                *   `... (files for other players) ...`
            *   `reddit_data_analyzed/` (Reddit mentions with VADER sentiment scores)
                *   `anthony_edwards_reddit_mentions_sentiment.csv`
                *   `... (files for other players) ...`
        *   `owaysCSV/` (Another set of Reddit mention CSVs - purpose to be clarified, possibly backup)
            *   `... (files for various players) ...`
    *   `fetch_reddit_data_comprehensive.py` (Reddit data scraping script)
    *   `Intro To DS Report NBA Sent Anal.pdf` (The project report document)
    *   `newNBAstats.py` (NBA stats related script)
    *   `Notebooks/`
        *   `actual final adadad.html` (HTML export of notebook)
        *   `actual final v2.html` (HTML export of notebook)
        *   `actual final v2.ipynb` (Working copy or version of the main analysis notebook)
        *   `actual final v3.html` (HTML export of notebook)
        *   `actual final.ipynb`
        *   `another notebook.ipynb`
        *   `nba_sentiment.ipynb`
        *   `not final.ipynb`
        *   `final but in parts/` (Notebook cells as separate Python scripts)
            *   `cell 1.py`
            *   `cell 3.py`
            *   `cell 4.py`
            *   `cell 5.py`
    *   `players.txt` (List of players studied)
    *   `PLEASEsentiment.py` (Script for VADER sentiment analysis)
    *   `process_all_players.py` (Script to merge stats and sentiment for all players)
    *   `process_sentiment_data.py` (Likely an earlier version or component of sentiment processing)
    *   `README.md` (This file)
    *   `sentimentmerger.py` (Script for merging sentiment with stats, likely used by `process_all_players.py`)
    *   `tree.txt` (The directory structure file itself)


## 6. Setup and Usage

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/VikramKad/NBA-Sentiment-Project.git
    cd NBA-Sentiment-Project
    ```
2.  **Set up a Python virtual environment (recommended):**
    ```bash
    python -m venv venv
    source venv/bin/activate  # On Windows: venv\Scripts\activate
    ```
3.  **Install dependencies:**
    Create a `requirements.txt` file with:
    ```
    pandas
    numpy
    matplotlib
    seaborn
    nltk
    praw
    scikit-learn
    ```
    
    You will also need to download the VADER lexicon:
    ```python
    import nltk
    nltk.download('vader_lexicon')
    ```
    (This can be done once in a Python interpreter or via your `PLEASEsentiment.py` script if it includes this).

4.  **Data Collection (if not already done):**
    *   Run the NBA stats fetching script.
    *   Configure PRAW with your Reddit API credentials.
    *   Run the Reddit scraping script(s).

5.  **Sentiment Analysis:**
    *   Execute `PLEASEsentiment.py` (or your equivalent script) to process raw Reddit data and add sentiment scores, saving to `data/new/reddit_data_analyzed/`.

6.  **Data Merging:**
    *   Execute `process_all_players.py` (or `sentimentmerger.py`) to combine player game logs with the aggregated sentiment data, producing files in `data/new/processed_data/`.

7.  **Run Analysis Notebook:**
    *   Open and run the main Jupyter Notebook (e.g., `nba_sentiment_unsupervised_analysis.ipynb`) to perform the exploratory data analysis, visualization, and clustering.

## 7. Key Findings (Summary from Report)

Our analysis of the combined performance and Reddit sentiment data for eight NBA stars across three seasons revealed several nuanced patterns:

*   **Game Outcome (Win/Loss):** There was a relatively weak positive association between a team win and higher average fan sentiment. Mean sentiment increased only slightly (approx. 0.15) for wins compared to losses. While median sentiment was higher for wins, the sentiment distribution for wins exhibited a wider spread and more lower outliers, suggesting varied reactions.
*   **Shooting Efficiency (FG\_PCT):** A weak positive correlation was observed between a player's field goal percentage and their mean sentiment score. More efficient shooting performances tended to receive slightly higher sentiment on average.
*   **Points Scored (PTS):** Surprisingly, a slight *negative* correlation was found between points scored and mean sentiment across the combined dataset. This counter-intuitive finding suggests that high point totals do not unilaterally lead to more positive sentiment and might be influenced by factors like game context (e.g., high scoring in losses, perceptions of "stat-padding") or high baseline expectations for star players. The data spread was notably wide.
*   **Recency of Posts (avg\_delta\_days):** A weak positive correlation was found between the average number of days separating a game and related Reddit posts (`avg_delta_days`) and the mean sentiment. This might suggest a "cool-off" effect, where sentiment becomes slightly more positive with more time for reflection.
*   **Plus-Minus (+/-):** No meaningful linear trend was observed between a player's plus-minus for a game and the mean sentiment.
*   **Other Metrics:** No strong, meaningful statistical correlations were found between sentiment metrics and other performance stats like assists or blocks in this aggregated view.

*(Note: Clustering results from the Jupyter Notebook would also be summarized here if they provide distinct insights into game-sentiment archetypes.)*

## 8. Limitations and Future Work

(Summarized from your report's "Discussion" and "Difference from Original Plan and Future Possibilities" sections)

*   **Data Source:** Limited to r/NBA; future work could include team-specific subreddits or other platforms like X.com.
*   **Sentiment Analysis Tool:** VADER has limitations; more advanced NLP models could be used.
*   **Contextual Factors:** Game importance, opponent quality, and media narratives were not explicitly modeled but likely influence sentiment.
*   **Player Reference Detection:** Current method relies on direct name/nickname mentions.

Future improvements could involve richer datasets, more sophisticated NLP, inclusion of contextual game data, and time-series analysis to model sentiment evolution.

## 9. Conclusion

This project successfully combined NBA performance data with Reddit sentiment to explore their relationship using unsupervised learning. While strong, direct linear correlations were not consistently found across all metrics, the analysis uncovered nuanced patterns, such as the unexpected negative trend between raw point totals and sentiment, and the impact of game outcome. The work highlights the complexity of fan sentiment and suggests that it's driven by more than just straightforward performance statistics, likely involving narrative, expectation, and game context.

---