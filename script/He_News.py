import requests
import time
import schedule
import datetime
import openai
import traceback
import sys
import os

sys.path.append(os.path.abspath(os.path.dirname(__file__)))
from HE_Database_Connect import get_connection

from HE_Error_Logs import log_error_to_db 

openai.api_key = "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"

stocks = ["AAPL", "TSLA", "SPY"]

def fetch_stock_news(stock_symbol):
    url = "https://query2.finance.yahoo.com/v1/finance/search"
    params = {"q": stock_symbol, "quotesCount": 0, "newsCount": 5}
    headers = {"User-Agent": "Mozilla/5.0"}

    try:
        response = requests.get(url, params=params, headers=headers)
        response.raise_for_status()
        data = response.json()
        return data.get("news", [])
    except Exception as e:
        error_description = traceback.format_exc()
        log_error_to_db("he_news.py", error_description, created_by="news_module")
        print(f" Error fetching news for {stock_symbol}: {e}")
        return []

def generate_summary(title, link):
    prompt = f"""
You are a financial news summarizer.
Given the following news headline and link, create a short 2-3 line summary.

Title: {title}
Link: {link}

Summary:
"""
    try:
        response = openai.ChatCompletion.create(
            model="gpt-3.5-turbo",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=100,
            temperature=0.5,
        )
        return response['choices'][0]['message']['content'].strip()
    except Exception as e:
        error_description = traceback.format_exc()
        log_error_to_db("he_news.py", error_description, created_by="news_module")
        print(f" Error generating summary: {e}")
        return "Summary not available."

def store_news_in_db(stock_symbol, title, summary, link, pub_time):
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO news_articles (stock_symbol, title, summary, link, pub_time)
            VALUES (%s, %s, %s, %s, %s)
        """, (stock_symbol, title, summary, link, pub_time))
        conn.commit()
    except Exception as e:
        error_description = traceback.format_exc()
        log_error_to_db("he_news.py", error_description, created_by="news_module")
        print(f" Database error: {e}")
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

def job():
    print(f"\n Fetching news at {time.strftime('%Y-%m-%d %H:%M:%S')}")
    for stock in stocks:
        print(f"\n Fetching news for {stock}")
        news_list = fetch_stock_news(stock)

        if news_list:
            for news in news_list:
                title = news.get('title', 'No Title')
                link = news.get('link', 'No Link')

                pub_time_ts = news.get('providerPublishTime')
                pub_time = datetime.datetime.utcfromtimestamp(pub_time_ts).strftime('%Y-%m-%d %H:%M:%S') if pub_time_ts else None

                summary = generate_summary(title, link)
                store_news_in_db(stock, title, summary, link, pub_time)

                print(f"\n Title       : {title}")
                print(f" Summary     : {summary}")
                print(f" Published at: {pub_time}")
                print(f" Link        : {link}\n")
        else:
            print(f" No news found for {stock}")


def main():
    try:
        job()  
        schedule.every(10).minutes.do(job)
        while True:
            schedule.run_pending()
            time.sleep(1)
    except Exception as e:
        error_description = traceback.format_exc()
        log_error_to_db("he_news.py", error_description, created_by="news_module")
        print(" Unhandled error occurred. Logged to database.")


if __name__ == "__main__":
    main()
