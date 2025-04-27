import os
import pandas as pd
import yfinance as yf
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score
import joblib

# Paths
SPARK_OUTPUT_DIR = "data/output"
STOCK_DATA_PATH = "data/AAPL_stock_data.csv"
MODEL_PATH = "data/model.pkl"

# Ensure output directory exists
os.makedirs(SPARK_OUTPUT_DIR, exist_ok=True)

# Download Apple stock data
ticker = yf.Ticker("AAPL")
hist = ticker.history(start="2007-12-01")
hist.reset_index(inplace=True)
hist['Date'] = hist['Date'].dt.strftime('%Y-%m-%d')
hist.to_csv(STOCK_DATA_PATH, index=False)

# Load Spark sentiment output
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
sentiment_df["date"] = pd.to_datetime(sentiment_df["date"], errors='coerce').dt.strftime("%Y-%m-%d")
sentiment_df = sentiment_df.dropna(subset=["date"])

# Aggregate sentiment per day
sentiment_summary = sentiment_df.groupby("date")["sentiment"].value_counts().unstack().fillna(0)
sentiment_summary["net_sentiment"] = sentiment_summary.get("Positive", 0) - sentiment_summary.get("Negative", 0)
sentiment_summary.reset_index(inplace=True)

# Merge with stock data
merged = pd.merge(hist, sentiment_summary, left_on="Date", right_on="date", how="inner")
merged["price_diff"] = merged["Close"].diff().shift(-1)
merged["target"] = merged["price_diff"].apply(lambda x: 1 if x > 0 else 0)

# Train Random Forest Model
X = merged[["net_sentiment"]].fillna(0)
y = merged["target"]

X_train, X_test, y_train, y_test = train_test_split(X, y, random_state=42)

clf = RandomForestClassifier()
clf.fit(X_train, y_train)

accuracy = accuracy_score(y_test, clf.predict(X_test))
print("✅ Model Accuracy:", accuracy)

# Save merged data
merged_path = os.path.join(SPARK_OUTPUT_DIR, "merged_sentiment.csv")
merged.to_csv(merged_path, index=False)

# Save model
joblib.dump(clf, MODEL_PATH)
