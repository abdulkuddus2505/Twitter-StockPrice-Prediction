from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType
import json
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

def analyze_sentiment(text):
    analyzer = SentimentIntensityAnalyzer()
    score = analyzer.polarity_scores(text)["compound"]
    if score >= 0.05:
        return "Positive"
    elif score <= -0.05:
        return "Negative"
    else:
        return "Neutral"

sentiment_udf = udf(analyze_sentiment, StringType())

spark = SparkSession.builder \
    .appName("TweetSentimentStreaming") \
    .getOrCreate()

df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "twitter_stream") \
    .load()

df_json = df_raw.selectExpr("CAST(value AS STRING)")

@udf(returnType=StringType())
def extract_text(json_string):
    return json.loads(json_string)["text"]

@udf(returnType=StringType())
def extract_date(json_string):
    return json.loads(json_string)["date"]

df_parsed = df_json.withColumn("text", extract_text(col("value"))) \
                   .withColumn("date", extract_date(col("value"))) \
                   .withColumn("sentiment", sentiment_udf(col("text")))

query = df_parsed.select("date", "text", "sentiment") \
    .writeStream \
    .format("csv") \
    .option("path", "data/output/") \
    .option("checkpointLocation", "data/checkpoint/") \
    .outputMode("append") \
    .start()

query.awaitTermination()
