import requests
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from HE_Database_Connect import get_connection 
from HE_Error_Logs import log_error_to_db      
nltk.download('vader_lexicon', quiet=True)


symbol = "TSLA"
fmp_api_key = "FAoCdSNAiQpDYc8C4HgcrOgqxXARc0nz"
url = f"https://financialmodelingprep.com/api/v3/income-statement/{symbol}?limit=1&apikey={fmp_api_key}"

class EarningsAnalyzer:
    def __init__(self, symbol, api_url):
        self.symbol = symbol
        self.api_url = api_url
        self.data = self.fetch_earnings_data()
        self.summary = self.generate_summary()
        self.vader = SentimentIntensityAnalyzer()

    def fetch_earnings_data(self):
        try:
            print(f"Requesting data for {self.symbol}...")
            response = requests.get(self.api_url)
            if response.status_code == 200:
                earnings_list = response.json()
                if isinstance(earnings_list, list) and len(earnings_list) > 0:
                    return earnings_list[0]
                else:
                    print("No earnings data found.")
            else:
                print(f"API request failed with status code {response.status_code}")
        except Exception as e:
            print(f"Error fetching earnings data: {e}")
            log_error_to_db("he_summarize_earings_report.py", str(e), created_by="fetch_earnings_data")
        return None

    def generate_summary(self):
        if self.data:
            try:
                return (
                    f"{self.symbol} reported earnings with the following key highlights:\n"
                    f"Date                : {self.data.get('date', 'N/A')}\n"
                    f"Total Revenue       : ${self.data.get('revenue', 'N/A'):,}\n"
                    f"Net Income          : ${self.data.get('netIncome', 'N/A'):,}\n"
                    f"EPS                 : {self.data.get('eps', 'N/A')}\n"
                    f"Operating Income    : ${self.data.get('operatingIncome', 'N/A'):,}\n"
                    f"Gross Profit        : ${self.data.get('grossProfit', 'N/A'):,}\n"
                    f"Operating Expenses  : ${self.data.get('operatingExpenses', 'N/A'):,}\n"
                    f"Cost of Revenue     : ${self.data.get('costOfRevenue', 'N/A'):,}\n"
                )
            except Exception as e:
                log_error_to_db("he_summarize_earings_report.py", str(e), created_by="generate_summary")
        return "No data available."

    def analyze_sentiment(self):
        try:
            return self.vader.polarity_scores(self.summary)
        except Exception as e:
            log_error_to_db("he_summarize_earings_report.py", str(e), created_by="analyze_sentiment")
            return {"compound": 0, "pos": 0, "neu": 1, "neg": 0}

    def sentiment_label(self, compound_score):
        if compound_score >= 0.05:
            return "Positive"
        elif compound_score <= -0.05:
            return "Negative"
        else:
            return "Neutral"

    def save_to_database(self):
        if not self.data:
            print("No data to save.")
            return
        
        sentiment = self.analyze_sentiment()
        try:
            conn = get_connection()
            cursor = conn.cursor()

            insert_query = """
            INSERT INTO earnings_data (
                symbol, date, revenue, net_income, eps, operating_income, gross_profit,
                operating_expenses, cost_of_revenue, sentiment_compound, sentiment_pos,
                sentiment_neu, sentiment_neg
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

            values = (
                self.symbol,
                self.data.get('date', 'N/A'),
                self.data.get('revenue', 0),
                self.data.get('netIncome', 0),
                self.data.get('eps', 0),
                self.data.get('operatingIncome', 0),
                self.data.get('grossProfit', 0),
                self.data.get('operatingExpenses', 0),
                self.data.get('costOfRevenue', 0),
                sentiment['compound'],
                sentiment['pos'],
                sentiment['neu'],
                sentiment['neg']
            )

            cursor.execute(insert_query, values)
            conn.commit()
            print(f"Data for {self.symbol} successfully saved to database.")
        except Exception as e:
            print(f"Error saving data to database: {e}")
            log_error_to_db("he_summarize_earings_report.py", str(e), created_by="save_to_database")
        finally:
            try:
                cursor.close()
                conn.close()
            except:
                pass

    def display_results(self):
        try:
            print("\n--- Earnings Summary ---\n")
            print(self.summary)

            sentiment = self.analyze_sentiment()
            sentiment_percentage = {
                "Negative": sentiment['neg'] * 100,
                "Neutral": sentiment['neu'] * 100,
                "Positive": sentiment['pos'] * 100,
                "Compound": sentiment['compound']
            }

            print("\n--- Sentiment Analysis ---")
            print(f"Sentiment    : Negative: {sentiment_percentage['Negative']:.2f}% ({sentiment['neg']:.4f}) | "
                  f"Neutral: {sentiment_percentage['Neutral']:.2f}% ({sentiment['neu']:.4f}) | "
                  f"Positive: {sentiment_percentage['Positive']:.2f}% ({sentiment['pos']:.4f}) | "
                  f"Compound: {sentiment_percentage['Compound']:.4f}")

            label = self.sentiment_label(sentiment['compound'])
            print(f"Final Sentiment Label: {label}")

            self.save_to_database()
        except Exception as e:
            log_error_to_db("he_summarize_earings_report.py", str(e), created_by="display_results")

if __name__ == "__main__":
    analyzer = EarningsAnalyzer(symbol, url)
    analyzer.display_results()
