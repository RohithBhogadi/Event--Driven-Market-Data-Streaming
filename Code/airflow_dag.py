# airflow_dag.py
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os
import json
import requests
from google.cloud import storage

# Set env variables for Airflow or use Secret Manager in production
os.environ["ALPHA_VANTAGE_KEY"] = "<YOUR_API_KEY>"
os.environ["GCS_BUCKET"] = "<your-bucket-name>"

# Config
ALPHA_API_KEY = os.getenv("ALPHA_VANTAGE_KEY")
GCS_BUCKET = os.getenv("GCS_BUCKET")
GCS_PREFIX = "market_stream/"

def get_intraday(symbol: str, interval: str = "1min"):
    url = "https://www.alphavantage.co/query"
    params = {
        "function": "TIME_SERIES_INTRADAY",
        "symbol": symbol,
        "interval": interval,
        "apikey": ALPHA_API_KEY,
        "outputsize": "compact"
    }
    resp = requests.get(url, params=params)
    resp.raise_for_status()
    data = resp.json()

    if "Time Series" not in str(data):
        raise ValueError(f"Unexpected API response: {data}")

    return data

def upload_to_gcs(data_str: str, blob_name: str):
    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET)
    blob = bucket.blob(f"{GCS_PREFIX}{blob_name}")
    blob.upload_from_string(data_str, content_type="application/json")
    print(f"Uploaded to gs://{GCS_BUCKET}/{GCS_PREFIX}{blob_name}")

def fetch_and_store(symbol: str):
    json_data = get_intraday(symbol)
    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    filename = f"{symbol}_intraday_{timestamp}.json"
    upload_to_gcs(data_str=json.dumps(json_data), blob_name=filename)

def run_pipeline():
    fetch_and_store("AAPL")

# Default arguments
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1)
}

# Define DAG
with DAG("market_data_to_gcs",
         default_args=default_args,
         schedule_interval="@hourly",
         catchup=False) as dag:

    fetch_data_task = PythonOperator(
        task_id="fetch_and_upload_market_data",
        python_callable=run_pipeline
    )

    fetch_data_task
