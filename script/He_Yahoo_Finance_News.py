import requests
from datetime import datetime
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from HE_Error_Logs import log_error_to_db 


try:
    nltk.download('vader_lexicon')
    sid = SentimentIntensityAnalyzer()
except Exception as e:
    log_error_to_db("he_yahoo_finance_news.py", str(e), created_by="nltk_setup")
    exit(" NLTK setup failed.")


def analyze_sentiment(text):
    try:
        scores = sid.polarity_scores(text)
        return scores
    except Exception as e:
        log_error_to_db("he_yahoo_finance_news.py", str(e), created_by="analyze_sentiment")
        return {}

url = (
    "https://www.alphavantage.co/query?function=NEWS_SENTIMENT&"
    "apikey=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"
)

try:
    response = requests.get(url, timeout=10)
    data = response.json()
except Exception as e:
    log_error_to_db("he_yahoo_finance_news.py", str(e), created_by="api_request")
    exit(" API request failed.")


if "feed" not in data:
    log_error_to_db("he_yahoo_finance_news.py", str(data), created_by="invalid_api_response")
    print("No news feed found. API response:", data)
    exit()


for article in data.get("feed", [])[:6]:
    try:
        raw_time = article.get("time_published", "")
        dt = datetime.strptime(raw_time, "%Y%m%dT%H%M%S")
        formatted_date = dt.strftime("%Y/%m/%d")
    except Exception as e:
        log_error_to_db("he_yahoo_finance_news.py", str(e), created_by="datetime_parse")
        formatted_date = "Invalid Date"

    try:
        summary = article.get("summary", "")
        ticker_data = article.get("ticker_sentiment", [])

        if not ticker_data:
            continue

        for item in ticker_data:
            ticker = item.get('ticker', 'N/A')
            relevance_score = item.get('relevance_score', 'N/A')
            ticker_sentiment_score_str = item.get('ticker_sentiment_score', '0')

            try:
                score = float(ticker_sentiment_score_str)
                sentiment_label = (
                    "Positive" if score > 0 else "Negative" if score < 0 else "Neutral"
                )
            except ValueError:
                sentiment_label = "Unknown"

           
            sentiment = analyze_sentiment(summary)

        
            print(f"Ticker: {ticker}")
            print(f"Relevance Score: {relevance_score}")
            print(f"Ticker Sentiment Score: {ticker_sentiment_score_str}")
            print(f"Inferred Sentiment: {sentiment_label}")
            print("Source:", article.get("source", "N/A"))
            print("Title:", article.get("title", "N/A"))
            print("Summary:", summary)
            print("Published At:", formatted_date)
            print("URL:", article.get("url", "N/A"))
            print(f" NLTK Sentiment (VADER): {sentiment}")
            print()

    except Exception as e:
        log_error_to_db("he_yahoo_finance_news.py", str(e), created_by="process_article")
        continue
