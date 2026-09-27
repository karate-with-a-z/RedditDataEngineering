from utils.constants import CLIENT_ID, OUTPUT_PATH, SECRET
from etls.reddit_etl import connect_reddit, extract_posts, transform_data, load_data_to_csv
import pandas as pd

def reddit_pipeline(file_name:str, subreddit_name:str,time_filter='day', limit= None):

    # Connecting to reddit instance
    instance = connect_reddit(CLIENT_ID, SECRET, 'Airscholar Agent')

    # Extraction
    if instance is None:
        print("Reddit instance is not initialized.")
    else:
        posts = extract_posts(reddit_instance=instance, subreddit_name=subreddit_name, time_filter=time_filter, limit=limit)
        post_df = pd.DataFrame(posts)

    # Transformation
        post_df = transform_data(post_df)

    # Loading to CSVs
        file_path = f'{OUTPUT_PATH}/{file_name}.csv'
        load_data_to_csv(post_df, file_path)
        return file_path