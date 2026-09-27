from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os .path.abspath(__file__))))

from pipelines.aws_s4_pipeline import upload_s3_pipeline
from pipelines.reddit_pipeline import reddit_pipeline


default_args = {
    'owner': 'Maritzabel',
    'start_date': datetime(year=2024, month=10, day=17)
}

file_postfix = datetime.now().strftime("%Y%m%d")

dag = DAG(
    dag_id = 'etls_reddit_pipeline',
    default_args= default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['reddit', 'etl', 'pipeline']
)


# Extraction from reddit
extract = PythonOperator(
    task_id = 'reddit_extraction',
    python_callable = reddit_pipeline,
    op_kwargs= {
        'file_name': f'reddit_{file_postfix}',
        'subreddit_name': 'dataengineering',
        'time_filer': 'day',
        'limit': 100
    },
    dag = dag

)

# upload to s3
upload_s3 = PythonOperator(
    task_id = 's3_upload',
    python_callable=upload_s3_pipeline,
    dag=dag,
)

extract >> upload_s3