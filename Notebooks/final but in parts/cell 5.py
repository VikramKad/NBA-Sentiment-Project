# ============================================================
# 5. COMBINED GRAPHS ACROSS ALL PLAYERS
#    (run once, after the per‑player loop finishes)
# ============================================================

print("\n\n🔗  BUILDING COMBINED DATASET FOR ALL PLAYERS ------------------")
combined_frames = []
for p_info in player_files_info:
    df_tmp = load_and_preprocess_data(p_info["file_path"], p_info["display_name"])
    if df_tmp is not None and not df_tmp.empty:
        df_tmp["PLAYER"] = p_info["display_name"]
        combined_frames.append(df_tmp)

if not combined_frames:
    print("❌ No player data available for combined analysis.")
else:
    df_all = pd.concat(combined_frames, ignore_index=True)
    print(f"Combined rows across players: {len(df_all)}")

    perf_cols  = ['PTS', 'AST', 'REB', 'PLUS_MINUS', 'FG_PCT']
    senti_cols = ['mean_sentiment', 'pos_share',
                  'neg_share' if 'neg_share' in df_all.columns else 'min_sentiment',
                  'post_count', 'avg_delta_days']

    key_pairs  = [('PTS', 'mean_sentiment'),
                  ('PLUS_MINUS', 'mean_sentiment'),
                  ('PTS', 'post_count'),
                  ('WIN', 'mean_sentiment') if 'WIN' in df_all.columns else None,
                  ('FG_PCT', 'mean_sentiment'),
                  ('mean_sentiment', 'avg_delta_days')]
    key_pairs = [p for p in key_pairs if p is not None]

    print("\n--- COMBINED UNIVARIATE DISTRIBUTIONS ---")
    plt.figure(figsize=(18, 10))
    plot_idx = 1
    for col_group in [perf_cols, senti_cols]:
        for col in col_group:
            if col in df_all.columns and not df_all[col].isnull().all():
                plt.subplot(2, max(len(perf_cols), len(senti_cols)), plot_idx)
                sns.histplot(df_all[col].dropna(), kde=True, bins=20)
                plt.title(f'Distribution of {col}')
                plot_idx += 1
    plt.tight_layout(rect=[0, 0, 1, 0.96])
    plt.suptitle('Univariate Distributions – ALL PLAYERS', y=1.00, fontsize=18)
    plt.show()

    print("\n--- COMBINED CORRELATION MATRIX ---")
    numeric_cols = [c for c in perf_cols + senti_cols
                    if c in df_all.columns and pd.api.types.is_numeric_dtype(df_all[c])]
    df_corr_all = df_all[numeric_cols].copy()
    df_corr_all = df_corr_all.apply(pd.to_numeric, errors='coerce')
    df_corr_all.dropna(how='all', axis=0, inplace=True)
    if len(df_corr_all) > 1:
        corr_mat = df_corr_all.corr()
        plt.figure(figsize=(14, 12))
        sns.heatmap(corr_mat, annot=True, cmap='coolwarm', fmt=".2f",
                    linewidths=.5, annot_kws={"size": 7})
        plt.title('Correlation Matrix – ALL PLAYERS', fontsize=16)
        plt.xticks(rotation=45, ha='right', fontsize=8)
        plt.yticks(rotation=0, fontsize=8)
        plt.tight_layout()
        plt.show()
    else:
        print("⚠️ Not enough data for combined correlation matrix.")

    print("\n--- COMBINED BIVARIATE RELATIONSHIPS ---")
    plt.figure(figsize=(20, 10))
    plot_idx = 1
    for x_col, y_col in key_pairs:
        if x_col in df_all.columns and y_col in df_all.columns:
            plt.subplot(2, 3, plot_idx)
            sns.scatterplot(data=df_all, x=x_col, y=y_col, hue='PLAYER', alpha=0.7)
            plt.title(f'{x_col} vs {y_col}')
            plot_idx += 1
    plt.tight_layout(rect=[0, 0, 1, 0.96])
    plt.suptitle('Key Relationships – ALL PLAYERS', y=1.00, fontsize=18)
    plt.show()

    print("\n--- MEAN VALUES (ALL PLAYERS) ---")
    mean_table_all = (
        df_all[numeric_cols]
        .mean().round(3)
        .to_frame(name='overall_mean')
        .sort_index()
    )
    print(mean_table_all)

print("\n\n🏁 All Players Processed! 🏁")