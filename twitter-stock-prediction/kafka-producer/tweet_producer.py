from kafka import KafkaProducer
import pandas as pd
import json
import time

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

df = pd.read_csv("data/twitter-data.csv",  delimiter=',').fillna("")
df = df.rename(columns={'Mon Apr 06 22:19:45 PDT 2009': "Date", "@switchfoot http://twitpic.com/2y1zl - Awww, that's a bummer.  You shoulda got David Carr of Third Day to do it. ;D": "Text"})
df = df[['Date', 'Text']]

for index, row in df.iterrows():
    message = {
        "date": row["Date"],
        "text": row["Text"]
    }
    producer.send('twitter_stream', value=message)
    print(f"Sent tweet {index + 1}")
    time.sleep(0.5)
