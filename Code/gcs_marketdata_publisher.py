# cloud_function-- gcs_marketdata_publisher.py
import base64
import json
import os
from google.cloud import pubsub_v1, storage

def gcs_to_pubsub(event, context):
    """
    Triggered by a change to a GCS bucket. Reads file and publishes to Pub/Sub
    """
    bucket_name = event['bucket']
    file_name = event['name']

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(os.getenv("GCP_PROJECT"), os.getenv("PUBSUB_TOPIC"))

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)

    content = blob.download_as_text()
    message = content.encode("utf-8")
    publisher.publish(topic_path, message)

    print(f"✅ Published {file_name} from GCS to Pub/Sub: {topic_path}")
