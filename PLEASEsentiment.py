import os
import csv
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer

# Ensure VADER lexicon is downloaded.
# If you haven't run your download_vader.py or done this manually,
# you might need to run this once:
# nltk.download('vader_lexicon')

def analyze_sentiment(text, analyzer):
    """
    Analyzes the sentiment of a given text using VADER.
    Returns a dictionary with neg, neu, pos, compound scores.
    Handles None or non-string inputs by returning default scores.
    """
    if not isinstance(text, str):
        text = "" # VADER handles empty strings returning all 0.0
    
    return analyzer.polarity_scores(text)

def process_csv_file(input_filepath, output_filepath, analyzer):
    """
    Reads a CSV file, performs sentiment analysis on specified columns,
    and writes the results to a new CSV file.
    """
    print(f"Processing {input_filepath}...")
    processed_rows = []
    
    # Define new header columns for sentiment scores
    sentiment_headers = []
    for col_prefix in ["title", "body", "comments"]:
        sentiment_headers.extend([
            f"{col_prefix}_neg", f"{col_prefix}_neu", 
            f"{col_prefix}_pos", f"{col_prefix}_compound"
        ])

    try:
        with open(input_filepath, 'r', encoding='utf-8-sig') as infile:
            reader = csv.reader(infile)
            header = next(reader)  # Read the header row
            
            # Store the indices of the columns to be analyzed
            try:
                title_col_idx = header.index("post_title")
                body_col_idx = header.index("post_body")
                comments_col_idx = header.index("scraped_comments_sample")
            except ValueError as e:
                print(f"  Error: Missing one of the required columns in {input_filepath}: {e}")
                print(f"  Expected columns: 'post_title', 'post_body', 'scraped_comments_sample'")
                print(f"  Found headers: {header}")
                return False

            new_header = header + sentiment_headers
            processed_rows.append(new_header)

            for row_num, row in enumerate(reader):
                # Ensure row has enough columns, pad with empty strings if not
                # This is a safeguard, ideally CSV rows are consistently structured
                while len(row) < max(title_col_idx, body_col_idx, comments_col_idx) + 1:
                    row.append("")

                post_title_text = row[title_col_idx]
                post_body_text = row[body_col_idx]
                scraped_comments_text = row[comments_col_idx]

                title_sentiment = analyze_sentiment(post_title_text, analyzer)
                body_sentiment = analyze_sentiment(post_body_text, analyzer)
                comments_sentiment = analyze_sentiment(scraped_comments_text, analyzer)

                new_row_parts = []
                for sentiment_dict in [title_sentiment, body_sentiment, comments_sentiment]:
                    new_row_parts.extend([
                        sentiment_dict['neg'], sentiment_dict['neu'],
                        sentiment_dict['pos'], sentiment_dict['compound']
                    ])
                
                processed_rows.append(row + new_row_parts)
        
        # Write the processed data to the output file
        with open(output_filepath, 'w', newline='', encoding='utf-8') as outfile:
            writer = csv.writer(outfile)
            writer.writerows(processed_rows)
        print(f"  Successfully processed. Output saved to {output_filepath}")
        return True

    except FileNotFoundError:
        print(f"  Error: File not found {input_filepath}")
        return False
    except Exception as e:
        print(f"  Error processing file {input_filepath}: {e}")
        return False

def main():
    # Define base directory (assuming script is in NBA-Sentiment-Project root)
    base_dir = os.getcwd() 
    
    input_dir = os.path.join(base_dir, "data", "new", "reddit_data")
    output_dir = os.path.join(base_dir, "data", "new", "reddit_data_analyzed")

    if not os.path.exists(input_dir):
        print(f"Input directory not found: {input_dir}")
        return

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"Created output directory: {output_dir}")

    try:
        analyzer = SentimentIntensityAnalyzer()
    except LookupError:
        print("VADER lexicon not found. Please run your download_vader.py script or nltk.download('vader_lexicon')")
        return

    for filename in os.listdir(input_dir):
        if filename.endswith(".csv"):
            input_filepath = os.path.join(input_dir, filename)
            output_filename = f"{os.path.splitext(filename)[0]}_sentiment.csv"
            output_filepath = os.path.join(output_dir, output_filename)
            
            process_csv_file(input_filepath, output_filepath, analyzer)

if __name__ == "__main__":
    main()