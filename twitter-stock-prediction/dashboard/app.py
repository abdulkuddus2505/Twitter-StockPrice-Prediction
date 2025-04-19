import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt

sentiment_df = pd.read_csv("data/output/merged_sentiment.csv")
stock_df = pd.read_csv("data/AAPL_stock_data.csv")

st.title("📈 Twitter Sentiment & Stock Prediction Dashboard")

st.write("Sentiment Overview")
st.bar_chart(sentiment_df.set_index("date")[["Positive", "Negative", "Neutral"]])

st.write("Stock Price Trend")
fig, ax = plt.subplots()
stock_df.set_index("Date")["Close"].plot(ax=ax)
st.pyplot(fig)
