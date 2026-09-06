from datetime import datetime, timedelta
import os
import json
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

DATA_LAKE_PATH = "/opt/airflow/data-lake"

def fetch_latest_stories(ds, **kwargs):
    url = "https://hacker-news.firebaseio.com/v0/newstories.json"
    response = requests.get(url)
    latest_ids = response.json()[:50]
    
    output_dir = os.path.join(DATA_LAKE_PATH, "raw", "stories", f"dt={ds}")
    os.makedirs(output_dir, exist_ok=True)
    
    stories_data = []
    for s_id in latest_ids:
        res = requests.get(f"https://hacker-news.firebaseio.com/v0/item/{s_id}.json")
        if res.status_code == 200 and res.json():
            stories_data.append(res.json())
            
    file_path = os.path.join(output_dir, "stories.json")
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(stories_data, f, ensure_ascii=False, indent=2)

def fetch_latest_comments(ds, **kwargs):
    url = "https://hacker-news.firebaseio.com/v0/maxitem.json"
    response = requests.get(url)
    max_id = response.json()
    
    output_dir = os.path.join(DATA_LAKE_PATH, "raw", "comments", f"dt={ds}")
    os.makedirs(output_dir, exist_ok=True)
    
    comments_data = []
    for item_id in range(max_id, max_id - 100, -1):
        res = requests.get(f"https://hacker-news.firebaseio.com/v0/item/{item_id}.json")
        if res.status_code == 200 and res.json():
            data = res.json()
            if data.get("type") == "comment":
                comments_data.append(data)
            if len(comments_data) >= 50:
                break
                
    file_path = os.path.join(output_dir, "comments.json")
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(comments_data, f, ensure_ascii=False, indent=2)

default_args = {
    'owner': 'fatma',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='hacker_news_ingestion',
    default_args=default_args,
    description='End-to-End Pipeline: Ingest, Process, and Analyze Hacker News Data',
    schedule_interval='@hourly',
    start_date=datetime(2026, 9, 1),
    catchup=False,
) as dag:

    # 1. Ingestion Tasks
    fetch_stories_task = PythonOperator(
        task_id='fetch_latest_stories',
        python_callable=fetch_latest_stories,
    )

    fetch_comments_task = PythonOperator(
        task_id='fetch_latest_comments',
        python_callable=fetch_latest_comments,
    )

    # 2. Transformation Tasks
    transform_stories_task = BashOperator(
        task_id='transform_stories_pyspark',
        bash_command='python /opt/airflow/scripts/transform_stories.py {{ ds }}',
    )

    transform_comments_task = BashOperator(
        task_id='transform_comments_pyspark',
        bash_command='python /opt/airflow/scripts/transform_comments.py {{ ds }}',
    )

    # 3. Analytics Task
    run_analytics_task = BashOperator(
        task_id='run_analytics_pyspark',
        bash_command='python /opt/airflow/scripts/run_analytics.py {{ ds }}',
    )

    # Dependencies
    fetch_stories_task >> transform_stories_task
    fetch_comments_task >> transform_comments_task
    [transform_stories_task, transform_comments_task] >> run_analytics_task