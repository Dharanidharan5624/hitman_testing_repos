
import requests
import time
import pandas as pd
import yfinance as yf
from tabulate import tabulate
import mysql.connector
from mysql.connector import Error
from email.message import EmailMessage
import smtplib
from datetime import datetime
from dateutil.relativedelta import relativedelta

# === Config ===
API_KEY = 'd0a8q79r01qnh1rh09v0d0a8q79r01qnh1rh09vg'
# Set start_date as datetime
start_date = datetime.today()

# Add one month
end_date = start_date + relativedelta(months=1)

# Convert to strings if needed
start_date_str = start_date.strftime('%Y-%m-%d')
end_date_str = end_date.strftime('%Y-%m-%d')



calendar_url = f'https://finnhub.io/api/v1/calendar/earnings?from={start_date}&to={end_date}&token={API_KEY}'
company_cache = {}

# Keywords to exclude policy/fund/ETF companies
EXCLUDE_KEYWORDS = ["fund", "trust", "etf", "reit", "insurance", "life", "portfolio"]

# Email credentials
sender_email = "dhineshapihitman@gmail.com"
receiver_email = "ila@shravtek.com,avinashgabadatasharing@gmail.com,bullseye.3578@gmail.com,sujit@shravtek.com","shreeram@shravtek.com","dharanidharan@shravtek.com"
subject = "Earnings Calendar Report"
app_password = "yiof ntnc xowc gpbp"

def convert_hour(hour_code):
    if not hour_code:
        return 'NULL'
    hour_code = hour_code.lower()
    return {
        'bmo': 'Before Market Open',
        'amc': 'After Market Close',
        'dmt': 'During Market Trading'
    }.get(hour_code, 'NULL')

def get_company_name(symbol):
    if symbol in company_cache:
        return company_cache[symbol]
    profile_url = f'https://finnhub.io/api/v1/stock/profile2?symbol={symbol}&token={API_KEY}'
    while True:
        try:
            response = requests.get(profile_url, timeout=5)
            if response.status_code == 200:
                data = response.json()
                name = data.get('name', 'N/A')
                company_cache[symbol] = name
                return name
            elif response.status_code == 429:
                print(f"Rate limit hit for {symbol}. Retrying in 60 sec...")
                time.sleep(60)
                continue
            else:
                return 'N/A'
        except Exception as e:
            print(f"Exception fetching profile for {symbol}: {e}")
            return 'N/A'

def get_actual_eps(symbol, earnings_date):
    earnings_url = f'https://finnhub.io/api/v1/stock/earnings?symbol={symbol}&token={API_KEY}'
    while True:
        try:
            response = requests.get(earnings_url, timeout=5)
            if response.status_code == 200:
                for record in response.json():
                    if record.get("period", "").startswith(earnings_date):
                        return record.get("actual", None)
                return None
            elif response.status_code == 429:
                print(f"Rate limit hit on earnings for {symbol}. Retrying in 60 sec...")
                time.sleep(60)
                continue
            else:
                return None
        except Exception as e:
            print(f"Error fetching actual EPS for {symbol}: {e}")
            return None

def get_last_year_eps(symbol, current_date):
    earnings_url = f'https://finnhub.io/api/v1/stock/earnings?symbol={symbol}&token={API_KEY}'
    current_year = int(current_date[:4])
    while True:
        try:
            response = requests.get(earnings_url, timeout=5)
            if response.status_code == 200:
                for record in response.json():
                    if record.get("period", "").startswith(str(current_year - 1)):
                        return record.get("actual", None)
                return None
            elif response.status_code == 429:
                print(f"Rate limit hit on earnings for {symbol}. Retrying in 60 sec...")
                time.sleep(60)
                continue
            else:
                return None
        except Exception as e:
            print(f"Error fetching last year EPS for {symbol}: {e}")
            return None

def create_mysql_connection():
    try:
        connection = mysql.connector.connect(
            host='localhost',
            user='Hitman',
            password='Hitman@123',
            database='hitman_edge_dev'
        )
        if connection.is_connected():
            print("✅ Connected to MySQL database")
            return connection
    except Error as e:
        print("❌ Error while connecting to MySQL:", e)
    return None

def format_market_cap(value):
    if value is None:
        return 'NULL'
    try:
        return f"${value / 1e9:.2f}B"
    except:
        return 'NULL'

def main():
    response = requests.get(calendar_url)
    if response.status_code != 200:
        print("Error fetching earnings calendar:", response.text)
        return

    earnings = response.json().get('earningsCalendar', [])
    if not earnings:
        print("No earnings data found.")
        return

    results = []
    total = len(earnings)

    for i, e in enumerate(earnings, 1):
        symbol = e.get('symbol', 'N/A')
        if symbol == 'N/A':
            continue

        try:
            ticker = yf.Ticker(symbol)
            info = ticker.info
            market_cap = info.get("marketCap", 0)
            if market_cap is None or market_cap < 1_000_000_000:
                print(f"⏩ Skipping {symbol}: Market cap < $1B")
                continue
           
            formatted_cap = format_market_cap(market_cap)
        except Exception as ex:
            print(f"❌ Error fetching market cap for {symbol}: {ex}")
            continue

        earnings_date = e.get('date', None)
        eps_estimate = e.get('epsEstimate', None)
        time_str = convert_hour(e.get('hour', 'N/A'))
        company_name = get_company_name(symbol)

        if any(keyword in company_name.lower() for keyword in EXCLUDE_KEYWORDS):
            print(f"⏩ Skipping {symbol} - {company_name} (excluded by keyword)")
            continue

        actual_eps = get_actual_eps(symbol, earnings_date) if earnings_date else None
        last_year_eps = get_last_year_eps(symbol, earnings_date) if earnings_date else None

        try:
            hist = ticker.history(period='1mo')
            current_price = hist['Close'].iloc[-1] if not hist.empty else None
            volatility = hist['Close'].pct_change().std() * (252**0.5) if not hist.empty else None
        except Exception as ex:
            print(f"⚠️ Error fetching price/volatility for {symbol}: {ex}")
            current_price = None
            volatility = None

        results.append({
            "Company Name": company_name or "NULL",
            "Ticker Symbol": symbol or "NULL",
            "Earnings Date": earnings_date or "NULL",
            "Time": time_str or "NULL",
            "EPS Estimate": eps_estimate if eps_estimate is not None else "NULL",
            "Actual EPS": actual_eps if actual_eps is not None else "NULL",
            "Last Year EPS": last_year_eps if last_year_eps is not None else "NULL",
            "Market Cap": formatted_cap,
            "Current Price" : f"${current_price:.2f}"if current_price else "NULL",
            "Volatility": f"{volatility:.2%}" if isinstance(volatility, float) else "NULL"
        })

        print(f"✅ Processed {i}/{total} - {symbol} | Market Cap: {formatted_cap}")
        time.sleep(0.2)

    if not results:
        print("ℹ️ No records to insert or email.")
        return

    df = pd.DataFrame(results)
    print(tabulate(df, headers='keys', tablefmt='pretty', showindex=False))

    conn = create_mysql_connection()
    if conn:
        try:
            insert_sql = '''
            INSERT INTO he_upcoming_earning_report (
                company_name, ticker_symbol, earnings_date, time, eps_estimate,
                actual_eps, market_cap, current_price, volatility
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            '''
            cursor = conn.cursor()
            values = [
                (row["Company Name"], row["Ticker Symbol"], row["Earnings Date"], row["Time"],
                 row["EPS Estimate"], row["Actual EPS"], row["Market Cap"],
                 row["Current Price"], row["Volatility"])
                for row in results
            ]
            cursor.executemany(insert_sql, values)
            conn.commit()
            print(f"✅ {cursor.rowcount} records inserted into MySQL.")
        except Exception as e:
            print("❌ Insert failed (check schema):", e)
        finally:
            conn.close()

    # Email - HTML body
    html_body = """
    <html>
    <head>
      <style>
        table { border-collapse: collapse; width: 100%; font-family: Arial, sans-serif; }
        th, td { border: 1px solid #ccc; padding: 8px; text-align: left; color: #000; }
        th { background-color: #f2f2f2; }
        h2 { color: #000; }
      </style>
    </head>
    <body>
      <h2>Earnings Calendar Report</h2>
      <table>
        <tr>
          <th>Company Name</th><th>Ticker</th><th>Date</th><th>Time</th>
          <th>Est. EPS</th><th>Actual EPS</th><th>Last Yr EPS</th>
          <th>Market Cap</th><th>Price</th><th>Volatility</th>
        </tr>
    """
    for row in results:
        html_body += f"""
        <tr>
          <td>{row['Company Name']}</td>
          <td>{row['Ticker Symbol']}</td>
          <td>{row['Earnings Date']}</td>
          <td>{row['Time']}</td>
          <td>{row['EPS Estimate']}</td>
          <td>{row['Actual EPS']}</td>
          <td>{row['Last Year EPS']}</td>
          <td>{row['Market Cap']}</td>
          <td>{row['Current Price']}</td>
          <td>{row['Volatility']}</td>
        </tr>
        """

    html_body += "</table></body></html>"

    msg = EmailMessage()
    msg["From"] = sender_email
    msg["To"] = receiver_email
    msg["Subject"] = subject
    msg.set_content("This is an HTML email. Please open in an HTML-compatible viewer.")
    msg.add_alternative(html_body, subtype='html')

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
            smtp.login(sender_email, app_password)
            smtp.send_message(msg)
        print("✅ Email sent successfully!")
    except Exception as e:
        print("❌ Email sending failed:", e)

if __name__ == "__main__":
    main()
