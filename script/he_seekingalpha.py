import sys
import os
import requests
import time
import json
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from HE_Database_Connect import get_connection
from HE_Error_Logs import log_error_to_db 
sys.path.append(os.path.abspath(os.path.dirname(__file__)))

nltk.download('vader_lexicon')
sid = SentimentIntensityAnalyzer()


def analyze_sentiment(text):
    try:
        scores = sid.polarity_scores(text)
        return scores
    except Exception as e:
        log_error_to_db("he_seekingalpha.py", str(e), created_by="analyze_sentiment")
        return {}

def store_article(symbols, title, summary, pub_time, link, sentiment_dict):
    try:
        conn = get_connection()
        cursor = conn.cursor()

        cursor.execute("SELECT COUNT(*) FROM news_articles WHERE link = %s", (link,))
        article_count = cursor.fetchone()[0]

        if article_count == 0:
            sentiment_json = json.dumps(sentiment_dict)
            insert_query = """
                INSERT INTO news_articles (stock_symbol, title, summary, pub_time, link, sentiment)
                VALUES (%s, %s, %s, %s, %s, %s)
            """
            symbols_str = ','.join(symbols)
            cursor.execute(insert_query, (symbols_str, title, summary, pub_time, link, sentiment_json))
            conn.commit()
            print(" Stored:", title[:60])
        else:
            print(f" Skipped (duplicate): {title[:60]} - Already exists.")

        cursor.close()
        conn.close()

    except Exception as e:
        print(" DB Error:", e)
        log_error_to_db("he_seekingalpha.py", str(e), created_by="store_article")


def fetch_article_details(article_id):
    try:
        url = f"https://seekingalpha.com/api/v3/news/{article_id}"
        headers = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}

        res = requests.get(url, headers=headers)
        if res.status_code == 200:
            data = res.json()

            attributes = data.get("data", {}).get("attributes", {})
            page = data.get("meta", {}).get("page", {})
            title = attributes.get("title", "No title")
            summary = page.get("description", "No summary")
            pub_time = attributes.get("publishOn", "Unknown")
            link = f"https://seekingalpha.com/news/{article_id}"

            relationships = data.get("data", {}).get("relationships", {})
            symbols_data = relationships.get("primaryTickers", {}).get("data", [])
            symbols = [s.get("id", "UNKNOWN") for s in symbols_data]

            sentiment = analyze_sentiment(summary)

            print(f" Symbols      : {symbols}")
            print(f"\n Title        : {title}")
            print(f" Summary      : {summary}")
            print(f" Published At : {pub_time}")
            print(f" Link         : {link}")
            print(f" Sentiment    : {sentiment}")
            print("-" * 80)

            store_article(symbols, title, summary, pub_time, link, sentiment)
        else:
            msg = f"Failed to fetch article {article_id}. Status code: {res.status_code}"
            print(msg)
            log_error_to_db("news_sentiment.py", msg, created_by="fetch_article_details")

    except Exception as e:
        log_error_to_db("he_seekingalpha.py", str(e), created_by="fetch_article_details")

def fetch_latest_news(limit=5):
    try:
        url = "https://seekingalpha.com/api/v3/news?filter[category]=market-news&page[size]=5&page[number]=1"
        headers = {
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json",
            "Accept-Language": "en-US,en;q=0.9"
        }

        res = requests.get(url, headers=headers)
        if res.status_code == 200:
            try:
                articles = res.json().get("data", [])[:limit]
                if not articles:
                    print(" No articles found.")
                    return

                for article in articles:
                    article_id = article.get("id")
                    if article_id:
                        fetch_article_details(article_id)
            except ValueError as e:
                msg = f"Error parsing response JSON: {e}"
                print(msg)
                log_error_to_db("news_sentiment.py", msg, created_by="fetch_latest_news")
        else:
            msg = f"Failed to fetch article list. Status code: {res.status_code}"
            print(msg)
            log_error_to_db("news_sentiment.py", msg, created_by="fetch_latest_news")
    except Exception as e:
        log_error_to_db("he_seekingalpha.py", str(e), created_by="fetch_latest_news")


if __name__ == "__main__":
    while True:
        try:
            print("\n Fetching latest news...\n")
            fetch_latest_news(limit=5)
            print(" Sleeping for 10 minutes...\n")
            time.sleep(600)
        except Exception as e:
            msg = f"Runtime Error: {e}"
            print(msg)
            log_error_to_db("he_seekingalpha.py", msg, created_by="main_loop")
            time.sleep(600)
