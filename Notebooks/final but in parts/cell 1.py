import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
from sklearn.cluster import KMeans

from pathlib import Path
import re

try:
    plt.style.use('seaborn-whitegrid')
except OSError:
    print("Style 'seaborn-whitegrid' not found, trying 'ggplot'.")
    plt.style.use('ggplot')

sns.set_context("talk")

SEASONS_STR = "2022_2024"
BASE_PROCESSED_DATA_DIR = Path("../data/new/processed_data/")

processed_files_pattern = f"*_stats_sentiment_{SEASONS_STR}.csv"
player_files_info = []

for f_path in BASE_PROCESSED_DATA_DIR.glob(processed_files_pattern):
    filename = f_path.name
    match = re.match(rf"(.+)_stats_sentiment_{SEASONS_STR}\.csv", filename)
    if match:
        player_slug = match.group(1)
        player_name_display = player_slug.replace('_', ' ').title()
        player_files_info.append({
            "slug": player_slug,
            "display_name": player_name_display,
            "file_path": f_path
        })
    else:
        match_plus = re.match(rf"(.+)_stats_plus_sentiment_{SEASONS_STR}\.csv", filename)
        if match_plus:
            player_slug = match_plus.group(1)
            player_name_display = player_slug.replace('_', ' ').title()
            player_files_info.append({
                "slug": player_slug,
                "display_name": player_name_display,
                "file_path": f_path
            })
        match_fullsnap = re.match(rf"(.+)_stats_plus_sentiment_fullsnap_{SEASONS_STR}\.csv", filename)
        if match_fullsnap:
            player_slug = match_fullsnap.group(1)
            player_name_display = player_slug.replace('_', ' ').title()
            player_files_info.append({
                "slug": player_slug,
                "display_name": player_name_display,
                "file_path": f_path
            })


if not player_files_info:
    print(f"❌ No processed player files found in {BASE_PROCESSED_DATA_DIR} matching the pattern '{processed_files_pattern}' or similar variations.")
    print("Please ensure 'process_all_players.py' has run and generated files like 'playername_stats_sentiment_2022_2024.csv'")
else:
    print(f"Found {len(player_files_info)} players to process:")
    for p_info in player_files_info:
        print(f"  - {p_info['display_name']} (File: {p_info['file_path'].name})")