# dataflow_pubsub_to_bq.py
import json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions

class ParseAndTransform(beam.DoFn):
    def process(self, element):
        message = element.decode('utf-8')
        data = json.loads(message)

        # Flattening the time series if needed (adjust this for your schema)
        results = []
        if "Time Series (1min)" in data:
            meta = data.get("Meta Data", {})
            symbol = meta.get("2. Symbol")
            for timestamp, values in data["Time Series (1min)"].items():
                results.append({
                    "symbol": symbol,
                    "timestamp": timestamp,
                    "open": float(values["1. open"]),
                    "high": float(values["2. high"]),
                    "low": float(values["3. low"]),
                    "close": float(values["4. close"]),
                    "volume": int(values["5. volume"])
                })
        return results


def run():
    options = PipelineOptions()
    options.view_as(StandardOptions).streaming = True
    
    project_id = "<YOUR_PROJECT_ID>"
    topic = "projects/<YOUR_PROJECT_ID>/topics/<YOUR_TOPIC_NAME>"
    bq_table = "<YOUR_PROJECT_ID>:<DATASET>.<TABLE_NAME>"

    schema = (
        'symbol:STRING,timestamp:TIMESTAMP,open:FLOAT,high:FLOAT,'
        'low:FLOAT,close:FLOAT,volume:INTEGER'
    )

    with beam.Pipeline(options=options) as p:
        (p
         | "Read from Pub/Sub" >> beam.io.ReadFromPubSub(topic=topic)
         | "Parse and Transform" >> beam.ParDo(ParseAndTransform())
         | "Write to BigQuery" >> beam.io.WriteToBigQuery(
                bq_table,
                schema=schema,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
            )
        )

if __name__ == '__main__':
    run()
