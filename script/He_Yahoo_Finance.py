import feedparser
from datetime import datetime
from urllib.parse import urlparse, parse_qs
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from HE_Error_Logs import log_error_to_db 


try:
    nltk.download('vader_lexicon', quiet=True)
    sid = SentimentIntensityAnalyzer()
except Exception as e:
    log_error_to_db("he_yahoo_finance.py", str(e), created_by="nltk_setup")
    exit(" NLTK setup failed.")


def analyze_sentiment(text):
    try:
        return sid.polarity_scores(text)
    except Exception as e:
        log_error_to_db("he_yahoo_finance.py", str(e), created_by="analyze_sentiment")
        return {"compound": 0.0}

def sentiment_label(compound_score):
    if compound_score >= 0.05:
        return "Positive"
    elif compound_score <= -0.05:
        return "Negative"
    else:
        return "Neutral"


def fetch_feed(symbol):
    try:
        url = f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={symbol}&region=US&lang=en-US"
        return feedparser.parse(url), url
    except Exception as e:
        log_error_to_db("he_yahoo_finance.py", str(e), created_by="fetch_feed")
        return None, None


def extract_tickers_from_url(url):
    try:
        parsed = urlparse(url)
        return parse_qs(parsed.query).get("s", ["Unknown"])[0].split(",")
    except Exception as e:
        log_error_to_db("he_yahoo_finance.py", str(e), created_by="extract_tickers")
        return ["Unknown"]


def parse_articles(feed, symbols, limit=7):
    count = 0
    for entry in feed.entries:
        if count >= limit:
            break
        try:
            summary = entry.summary
            sentiment_scores = analyze_sentiment(summary)
            label = sentiment_label(sentiment_scores["compound"])

            print(f"Tickers: {', '.join(symbols)}")
            print(f"Published: {entry.published}")
            print(f"Title: {entry.title}")
            print(f"Summary: {summary}")
            print(f"Link: {entry.link}")
            print(f"Sentiment Score: {sentiment_scores}")
            print(f"Final Sentiment Label: {label}")
            print("-" * 60)
            count += 1
        except Exception as e:
            log_error_to_db("he_yahoo_finance.py", str(e), created_by="parse_articles")
            continue


def main():
    symbol = "HIMS" 
    feed, url = fetch_feed(symbol)
    if not feed or not feed.entries:
        log_error_to_db("rss_news_sentiment.py", f"No feed or entries for {symbol}", created_by="main")
        print(f"No articles found for {symbol}")
        return

    symbols = extract_tickers_from_url(url)
    print(f"Analyzing latest news for {symbol}...\n")
    parse_articles(feed, symbols, limit=7)

if __name__ == "__main__":
    main()
