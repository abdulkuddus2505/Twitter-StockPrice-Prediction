from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType
import json
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# Sentiment Analysis Function
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

# Spark Session
spark = SparkSession.builder \
    .appName("TweetSentimentStreaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:3.3.1") \
    .getOrCreate()

# Read from Kafka
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "twitter_stream") \
    .load()

df_json = df_raw.selectExpr("CAST(value AS STRING)")

# UDFs to extract text and date
@udf(returnType=StringType())
def extract_text(json_string):
    return json.loads(json_string)["text"]

@udf(returnType=StringType())
def extract_date(json_string):
    return json.loads(json_string)["date"]

# Parse and add sentiment
df_parsed = df_json.withColumn("text", extract_text(col("value"))) \
                   .withColumn("date", extract_date(col("value"))) \
                   .withColumn("sentiment", sentiment_udf(col("text")))

# Write to CSV
query = df_parsed.select("date", "text", "sentiment") \
    .writeStream \
    .format("csv") \
    .option("path", "data/output/") \
    .option("checkpointLocation", "data/checkpoint/") \
    .outputMode("append") \
    .start()

query.awaitTermination()
