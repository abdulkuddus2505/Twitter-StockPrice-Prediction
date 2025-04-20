from kafka import KafkaProducer
from google.cloud import storage
import pandas as pd
import json
import io
import time


# === Google Cloud Storage Configuration ===
bucket_name = "woodstock-twitter-stock"
blob_name = "twitter-data.csv"  # e.g., "twitter_data/twitter-data.csv"

# Initialize GCS client
client = storage.Client()
bucket = client.bucket(bucket_name)
blob = bucket.blob(blob_name)

# Download the CSV content as string
csv_bytes = blob.download_as_bytes()
csv_data = csv_bytes.decode('utf-8', errors='replace') 

df = pd.read_csv(io.StringIO(csv_data), delimiter=',').fillna("")
df = df.rename(columns={
    'Mon Apr 06 22:19:45 PDT 2009': "Date",
    "@switchfoot http://twitpic.com/2y1zl - Awww, that's a bummer.  You shoulda got David Carr of Third Day to do it. ;D": "Text"
})
df = df[['Date', 'Text']]


producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)


for index, row in df.iterrows():
    message = {
        "date": row["Date"],
        "text": row["Text"]
    }
    producer.send('twitter_stream', value=message)
    print(f"Sent tweet {index + 1}")
    time.sleep(0.5)
