import os
import pandas as pd
import yfinance as yf
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score

# Get base path to the sibling data directory
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SPARK_OUTPUT_DIR = os.path.join(BASE_DIR, "spark_sentiment", "data", "output")
STOCK_DATA_PATH = os.path.join(BASE_DIR, "spark_sentiment", "data", "AAPL_stock_data.csv")

print("Spark Output Dir", SPARK_OUTPUT_DIR)

print("Stock Data Path", STOCK_DATA_PATH)

# Ensure output directory exists
os.makedirs(SPARK_OUTPUT_DIR, exist_ok=True)

ticker = yf.Ticker("AAPL")
hist = ticker.history(start="2007-12-01")
hist.reset_index(inplace=True)
hist['Date'] = hist['Date'].dt.strftime('%Y-%m-%d')
print("History", hist.head())
hist.to_csv(STOCK_DATA_PATH, index=False)

# === 2. Load processed sentiment data from Spark output ===
sentiment_files = [
    f for f in os.listdir(SPARK_OUTPUT_DIR)
    if f.endswith(".csv") and f != "merged_sentiment.csv"
]

sentiment_dataframes = []
for file in sentiment_files:
    file_path = os.path.join(SPARK_OUTPUT_DIR, file)
    try:
        df = pd.read_csv(file_path, names=["date", "text", "sentiment"])
        if not df.empty:
            sentiment_dataframes.append(df)
    except pd.errors.EmptyDataError:
        print(f"Skipped empty file: {file_path}")

if not sentiment_dataframes:
    raise ValueError("No valid sentiment data found. All files were empty.")

sentiment_df = pd.concat(sentiment_dataframes, ignore_index=True)
sentiment_df["date"] = pd.to_datetime(sentiment_df["date"]).dt.strftime("%Y-%m-%d")

print("🗓️ Sentiment Data Range:")
print("Start Date:", sentiment_df["date"].min())
print("End Date:", sentiment_df["date"].max())
# === 3. Aggregate sentiment per day ===
sentiment_summary = sentiment_df.groupby("date")["sentiment"].value_counts().unstack().fillna(0)
sentiment_summary["net_sentiment"] = sentiment_summary.get("Positive", 0) - sentiment_summary.get("Negative", 0)
sentiment_summary.reset_index(inplace=True)

# === 4. Merge with stock data ===
merged = pd.merge(hist, sentiment_summary, left_on="Date", right_on="date", how="inner")
merged["price_diff"] = merged["Close"].diff().shift(-1)
merged["target"] = merged["price_diff"].apply(lambda x: 1 if x > 0 else 0)

# === 5. Train Random Forest model ===
X = merged[["net_sentiment"]].fillna(0)
y = merged["target"]

print("X shape", X.shape)
print("y shape", y.shape)

X_train, X_test, y_train, y_test = train_test_split(X, y, random_state=42)

clf = RandomForestClassifier()
clf.fit(X_train, y_train)

accuracy = accuracy_score(y_test, clf.predict(X_test))
print("Model Accuracy:", accuracy)

# === 6. Save merged file for dashboard ===
merged_path = os.path.join(SPARK_OUTPUT_DIR, "merged_sentiment.csv")
merged.to_csv(merged_path, index=False)
