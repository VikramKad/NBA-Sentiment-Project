if not player_files_info:
    print("No player files were found to process. Halting execution.")
else:
    for player_info in player_files_info:
        PLAYER_SLUG = player_info["slug"]
        PLAYER_NAME_DISPLAY = player_info["display_name"]
        input_file_path = player_info["file_path"]

        print(f"\n\n{'='*20} PROCESSING: {PLAYER_NAME_DISPLAY} {'='*20}")
        
        df_player = load_and_preprocess_data(input_file_path, PLAYER_NAME_DISPLAY)

        if df_player is None or df_player.empty:
            print(f"--- Skipping further analysis for {PLAYER_NAME_DISPLAY} due to missing data or no games with sentiment ---")
            continue

        print(f"\n--- Univariate Distributions for {PLAYER_NAME_DISPLAY} ---")
        performance_features_to_plot = ['PTS', 'AST', 'REB', 'PLUS_MINUS', 'FG_PCT']
        sentiment_features_to_plot = ['mean_sentiment', 'pos_share', 
                                      'neg_share' if 'neg_share' in df_player.columns else 'min_sentiment', 
                                      'post_count', 'avg_delta_days']
        
        plt.figure(figsize=(18, 10))
        plot_idx = 1
        for col_list, sup_title_part in [(performance_features_to_plot, "Performance"), (sentiment_features_to_plot, "Sentiment Metrics")]:
            for col in col_list:
                if col in df_player.columns and not df_player[col].isnull().all():
                    plt.subplot(2, max(len(performance_features_to_plot), len(sentiment_features_to_plot)), plot_idx)
                    sns.histplot(df_player[col].dropna(), kde=True, bins=15)
                    plt.title(f'Distribution of {col}')
                    plt.xlabel(col)
                    plt.ylabel('Frequency')
                    plot_idx +=1
                else:
                    print(f"  Skipping histogram for missing/empty column: {col}")
        plt.tight_layout(rect=[0, 0, 1, 0.96])
        plt.suptitle(f'Univariate Distributions for {PLAYER_NAME_DISPLAY}', y=1.00, fontsize=16)
        plt.show()

        print(f"\n--- Correlation Analysis for {PLAYER_NAME_DISPLAY} ---")
        numerical_stats_cols = ['PTS', 'REB', 'AST', 'STL', 'BLK', 'TOV', 'FG_PCT', 'FG3_PCT', 'FT_PCT', 'PLUS_MINUS', 'WIN', 'MIN']
        numerical_sentiment_cols = ['mean_sentiment', 'min_sentiment', 'max_sentiment', 'pos_share', 
                                    'neg_share' if 'neg_share' in df_player.columns else None, 
                                    'post_count', 'avg_delta_days', 'min_delta_days', 'max_delta_days']
        numerical_sentiment_cols = [col for col in numerical_sentiment_cols if col is not None]

        correlation_features = [col for col in numerical_stats_cols if col in df_player.columns and pd.api.types.is_numeric_dtype(df_player[col])] + \
                               [col for col in numerical_sentiment_cols if col in df_player.columns and pd.api.types.is_numeric_dtype(df_player[col])]
        
        df_corr = df_player[list(set(correlation_features))].copy()
        
        for col in df_corr.columns:
            df_corr[col] = pd.to_numeric(df_corr[col], errors='coerce')
        df_corr.dropna(inplace=True)

        if len(df_corr) > 1 and len(df_corr.columns) > 1:
            correlation_matrix = df_corr.corr()
            plt.figure(figsize=(18, 15))
            sns.heatmap(correlation_matrix, annot=True, cmap='coolwarm', fmt=".2f", linewidths=.5, annot_kws={"size": 7})
            plt.title(f'Correlation Matrix for {PLAYER_NAME_DISPLAY}', fontsize=16)
            plt.xticks(rotation=45, ha='right', fontsize=8)
            plt.yticks(rotation=0, fontsize=8)
            plt.tight_layout()
            plt.show()
        else:
            print("  ⚠️ Not enough data or columns for correlation matrix.")

        print(f"\n--- Unsupervised Clustering for {PLAYER_NAME_DISPLAY} ---")

        cluster_features = [
            'PTS', 'AST', 'REB', 'PLUS_MINUS',
            'mean_sentiment', 'sent_intensity'
        ]
        cluster_features = [c for c in cluster_features if c in df_player.columns]

        X = df_player[cluster_features].fillna(0)
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X)

        pca = PCA(n_components=2, random_state=42)
        X_pca = pca.fit_transform(X_scaled)
        print("   • PCA two‑component variance:",
              round(pca.explained_variance_ratio_.sum(), 3))

        inertias = []
        for k in range(1, 8):
            inertias.append(
                KMeans(n_clusters=k, n_init=10, random_state=42)
                .fit(X_scaled).inertia_
            )
        plt.figure(figsize=(4, 3))
        plt.plot(range(1, 8), inertias, marker='o')
        plt.title('Elbow for K‑means'); plt.xlabel('k'); plt.ylabel('Inertia')
        plt.show()

        k = 3
        kmeans = KMeans(n_clusters=k, n_init=10, random_state=42)
        df_player['cluster'] = kmeans.fit_predict(X_scaled)

        plt.figure(figsize=(7, 6))
        sns.scatterplot(x=X_pca[:, 0], y=X_pca[:, 1],
                        hue=df_player['cluster'], palette='Set2', s=80)
        plt.title(f'PCA space – {PLAYER_NAME_DISPLAY}')
        plt.xlabel('PC1'); plt.ylabel('PC2')
        plt.legend(title='Cluster')
        plt.show()

        profile_cols = cluster_features + (['WIN'] if 'WIN' in df_player.columns else [])
        print("Cluster medians:")
        display(
            df_player.groupby('cluster')[profile_cols]
                     .median()
                     .round(2)
        )

        print(f"\n--- Bivariate Scatter Plots for {PLAYER_NAME_DISPLAY} ---")
        key_relationships = [
            ('PTS', 'mean_sentiment'), ('PLUS_MINUS', 'mean_sentiment'),
            ('PTS', 'post_count'), ('WIN', 'mean_sentiment'),
            ('FG_PCT', 'mean_sentiment'), ('mean_sentiment', 'avg_delta_days')
        ]
        plt.figure(figsize=(20, 10))
        plot_idx = 1
        for x_col, y_col in key_relationships:
            if x_col in df_player.columns and y_col in df_player.columns:
                plt.subplot(2, 3, plot_idx)
                if x_col == 'WIN' and not df_player[x_col].isnull().all():
                    sns.boxplot(x=df_player[x_col].astype(int), y=df_player[y_col].dropna(), palette={0: 'red', 1: 'green'})
                    plt.xticks([0,1], ['Loss', 'Win'])
                elif not df_player[x_col].isnull().all() and not df_player[y_col].isnull().all():
                    sns.scatterplot(data=df_player, x=x_col, y=y_col, hue='WIN' if 'WIN' in df_player.columns else None, 
                                    palette={0: 'red', 1: 'green'} if 'WIN' in df_player.columns else None, alpha=0.7)
                    try:
                        sns.regplot(data=df_player, x=x_col, y=y_col, scatter=False, color='blue', line_kws={'linestyle':'--'})
                    except ValueError:
                        print(f"Could not plot regression line for {x_col} vs {y_col} (possibly due to low variance or NaNs).")
                plt.title(f'{x_col} vs. {y_col}')
                plot_idx += 1
            else:
                 print(f"  Skipping bivariate plot: missing {x_col} or {y_col}")
        plt.tight_layout(rect=[0, 0, 1, 0.96])
        plt.suptitle(f'Key Bivariate Relationships for {PLAYER_NAME_DISPLAY}', y=1.00, fontsize=18)
        plt.show()

        if 'WIN' in df_player.columns and not df_player['WIN'].isnull().all():
            print(f"\n--- Sentiment Analysis by Game Outcome for {PLAYER_NAME_DISPLAY} ---")
            plt.figure(figsize=(18, 6))
            sentiment_metrics_for_outcome = ['mean_sentiment', 'pos_share', 
                                             'neg_share' if 'neg_share' in df_player.columns else 'min_sentiment', 
                                             'post_count']
            plot_idx = 1
            for metric in sentiment_metrics_for_outcome:
                if metric in df_player.columns and not df_player[metric].isnull().all():
                    plt.subplot(1, len(sentiment_metrics_for_outcome), plot_idx)
                    sns.boxplot(x='WIN', y=metric, data=df_player, palette={0: 'salmon', 1: 'lightgreen'})
                    plt.title(f'{metric} by Outcome')
                    plt.xticks([0, 1], ['Loss', 'Win'])
                    plt.xlabel('Game Outcome')
                    plt.ylabel(metric.replace('_', ' ').title())
                    plot_idx +=1
                else:
                    print(f"  Skipping outcome plot for missing/empty column: {metric}")
            plt.tight_layout(rect=[0, 0, 1, 0.96])
            plt.suptitle(f'Sentiment Metrics by Game Outcome for {PLAYER_NAME_DISPLAY}', y=1.00, fontsize=16)
            plt.show()
        else:
            print(f"  Skipping sentiment by game outcome for {PLAYER_NAME_DISPLAY} (WIN column missing or all NaN).")
        
        print(f"\n--- Finished processing {PLAYER_NAME_DISPLAY} ---")



