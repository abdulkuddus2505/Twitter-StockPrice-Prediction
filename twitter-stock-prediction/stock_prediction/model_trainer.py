import pandas as pd
import yfinance as yf
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score

# Download stock data
ticker = yf.Ticker("AAPL")
hist = ticker.history(period="5y")
hist.reset_index(inplace=True)
hist['Date'] = hist['Date'].dt.strftime('%Y-%m-%d')
hist.to_csv("data/AAPL_stock_data.csv", index=False)

# Load processed sentiment data (from Spark output)
sentiment_df = pd.concat([
    pd.read_csv(f"data/output/{file}")
    for file in os.listdir("data/output")
    if file.endswith(".csv")
], ignore_index=True, names=["date", "text", "sentiment"])

sentiment_summary = sentiment_df.groupby("date")["sentiment"].value_counts().unstack().fillna(0)
sentiment_summary["net_sentiment"] = sentiment_summary.get("Positive", 0) - sentiment_summary.get("Negative", 0)
sentiment_summary.reset_index(inplace=True)

# Merge with stock data
merged = pd.merge(hist, sentiment_summary, left_on="Date", right_on="date", how="inner")
merged["price_diff"] = merged["Close"].diff().shift(-1)
merged["target"] = merged["price_diff"].apply(lambda x: 1 if x > 0 else 0)

X = merged[["net_sentiment"]].fillna(0)
y = merged["target"]
X_train, X_test, y_train, y_test = train_test_split(X, y)

clf = RandomForestClassifier()
clf.fit(X_train, y_train)

accuracy = accuracy_score(y_test, clf.predict(X_test))
print("Model Accuracy:", accuracy)
