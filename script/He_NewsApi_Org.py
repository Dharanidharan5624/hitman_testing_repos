import requests
from textblob import TextBlob
from datetime import date, timedelta
import time
import traceback

from HE_Database_Connect import get_connection
from HE_Error_Logs import  log_error_to_db

API_KEY = '6a2e7b8388724ec7b7420c74d3bb2844'
symbol = 'AAPL'
interval_minutes = 10
interval_seconds = interval_minutes * 60


def get_sentiment(text):
    blob = TextBlob(text)
    polarity = blob.sentiment.polarity
    if polarity > 0.1:
        return "Positive"
    elif polarity < -0.1:
        return "Negative"
    else:
        return "Neutral"


while True:
    try:
        today = date.today()
        yesterday = today - timedelta(days=1)

        url = (
            f'https://newsapi.org/v2/everything?q={symbol}'
            f'&from={yesterday.isoformat()}'
            f'&to={today.isoformat()}'
            f'&apiKey={API_KEY}&language=en&sortBy=publishedAt'
        )

        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        print(f"\n Top news articles for: {symbol} from {yesterday} to {today}\n")

        if 'articles' in data and data['articles']:
            for article in data['articles'][:5]:
                try:
                    title = article.get('title', '')
                    description = article.get('description', '')
                    published_at = article.get('publishedAt', '')
                    url = article.get('url', '')

                    sentiment = get_sentiment(f"{title} {description}")

                    print(f" Symbol       : {symbol}")
                    print(f" Title        : {title}")
                    print(f" Description  : {description}")
                    print(f" Published At : {published_at}")
                    print(f" URL          : {url}")
                    print(f" Sentiment    : {sentiment}")
                    print("-" * 100)

                  

                except Exception as inner_err:
                    log_error_to_db("he_newsapi_org.py", traceback.format_exc(), created_by="sentiment_loop")
        else:
            print(" No articles found for yesterday/today or an error occurred.")

    except Exception as e:
        error_description = traceback.format_exc()
        log_error_to_db("he_newsapi_org.py", error_description, created_by="sentiment_loop")
        print(f" Error fetching or processing news: {e}")

    time.sleep(interval_seconds)
