def load_and_preprocess_data(file_path, player_name_display):
    """Loads and preprocesses the combined stats and sentiment data for a player."""
    if not file_path.exists():
        print(f"❌ ERROR: Data file not found at {file_path} for {player_name_display}")
        return None

    print(f"\n--- Loading data for {player_name_display} from {file_path.name} ---")
    df = pd.read_csv(file_path)
    print(f"Initial rows loaded: {len(df)}")

    if 'game_date' not in df.columns:
        # Fallback if 'game_date' was not correctly named/saved by previous script, try 'GAME_DATE'
        if 'GAME_DATE' in df.columns:
            print("Using 'GAME_DATE' and converting to datetime.")
            df['game_date'] = pd.to_datetime(df['GAME_DATE'], errors='coerce')
        else:
            print(f"❌ ERROR: Neither 'game_date' nor 'GAME_DATE' column found for {player_name_display}.")
            return None
    else:
        df['game_date'] = pd.to_datetime(df['game_date'], errors='coerce')
    
    df.dropna(subset=['game_date'], inplace=True)

    if 'WL' in df.columns:
        df['WIN'] = df['WL'].apply(lambda x: 1 if x == 'W' else 0 if x == 'L' else np.nan)
        df.dropna(subset=['WIN'], inplace=True)
        df['WIN'] = df['WIN'].astype(int)
    else:
        print(f"⚠️ WL column not found for {player_name_display}. Cannot create WIN feature.")
        df['WIN'] = np.nan # Add WIN column as NaN if WL is missing

    print(f"Rows before filtering on post_count: {len(df)}")
    if 'post_count' in df.columns:
        df_filtered = df[df['post_count'] > 0].copy()
        print(f"Rows after filtering for post_count > 0: {len(df_filtered)}")
        if len(df_filtered) == 0:
            print(f"⚠️ No games found with post_count > 0 for {player_name_display}. Sentiment analysis will be limited.")
            # Return the original df if no posts, so at least stats can be analyzed if desired (optional)
            # For this project, we require sentiment, so we'll return None if no sentiment games
            return None 
        print(f"Filtered data to {len(df_filtered)} games with sentiment (post_count > 0).")
    else:
        print(f"❌ ERROR: 'post_count' column not found for {player_name_display}. Cannot filter for games with sentiment.")
        return None

    if {'mean_sentiment', 'post_count'}.issubset(df_filtered.columns):
        df_filtered['sent_intensity'] = (
            df_filtered['mean_sentiment'] * df_filtered['post_count']
        )
    else:
        df_filtered['sent_intensity'] = np.nan
        
    return df_filtered

print("Helper function 'load_and_preprocess_data' defined.")