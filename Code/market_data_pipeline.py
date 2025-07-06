
import os
import json
import requests
from datetime import datetime
from google.cloud import storage

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
    print(f"✅ Uploaded to gs://{GCS_BUCKET}/{GCS_PREFIX}{blob_name}")

def fetch_and_store(symbol: str):
    try:
        json_data = get_intraday(symbol)
        timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        filename = f"{symbol}_intraday_{timestamp}.json"
        upload_to_gcs(data_str=json.dumps(json_data), blob_name=filename)
    except Exception as e:
        print(f"❌ Failed for {symbol}: {e}")

if __name__ == "__main__":
    fetch_and_store("AAPL")
