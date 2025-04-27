import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import joblib

# Load Data
sentiment_df = pd.read_csv("data/output/merged_sentiment.csv")
stock_df = pd.read_csv("data/AAPL_stock_data.csv")
model = joblib.load("data/model.pkl")

st.title("📈 Twitter Sentiment & Stock Prediction Dashboard")

st.write("### Sentiment Overview")
st.bar_chart(sentiment_df.set_index("date")[["Positive", "Negative", "Neutral"]])

st.write("### Stock Price Trend")
fig, ax = plt.subplots()
stock_df.set_index("Date")["Close"].plot(ax=ax)
st.pyplot(fig)

# === NEW ===
st.write("### 📊 Predict Stock Movement")

# Options for user
option = st.selectbox(
    "How would you like to predict?",
    ("Enter a Date", "Enter Net Sentiment Manually")
)

if option == "Enter a Date":
    input_date = st.text_input("Enter Date (YYYY-MM-DD)")
    if input_date:
        filtered = sentiment_df[sentiment_df['date'] == input_date]
        if not filtered.empty:
            net_sentiment = filtered['Positive'].values[0] - filtered['Negative'].values[0]
            prediction = model.predict([[net_sentiment]])[0]
            st.success(f"📅 On {input_date}, Predicted Stock Movement: {'UP 📈' if prediction == 1 else 'DOWN 📉'}")
        else:
            st.error("No sentiment data available for this date.")

else:
    net_sentiment_input = st.number_input("Enter Net Sentiment Value (e.g., 5, -3, etc.)", step=1)
    if st.button("Predict"):
        prediction = model.predict([[net_sentiment_input]])[0]
        st.success(f"🔮 Predicted Stock Movement: {'UP 📈' if prediction == 1 else 'DOWN 📉'}")
