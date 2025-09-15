import pandas as pd
from collections import deque, defaultdict
from datetime import datetime
from decimal import Decimal, InvalidOperation
import yfinance as yf
from tabulate import tabulate
from HE_Database_Connect import get_connection
from HE_Error_Logs import log_error_to_db

# ------------------ Pandas Display Settings ------------------
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 200)

# ------------------ Helper Functions ------------------
def safe_round(val, digits=2):
    try:
        return round(float(val), digits)
    except (ValueError, TypeError):
        return 0.0

def safe_divide(numerator, denominator):
    try:
        return float(numerator) / float(denominator) if float(denominator) != 0 else 0.0
    except (ValueError, TypeError, ZeroDivisionError):
        return 0.0

def safe_info_value(info, key, default=0.0):
    try:
        val = info.get(key)
        return float(val) if val is not None else default
    except (ValueError, TypeError):
        return default

# ------------------ FIFO Processing ------------------
def process_fifo(transactions):
    fifo_queue = deque()
    realized_gain = Decimal('0')
    total_quantity = Decimal('0')
    total_cost = Decimal('0')

    for t in transactions:
        try:
            quantity = Decimal(t['quantity'])
            price = Decimal(t['price'])
        except InvalidOperation:
            log_error_to_db("HE_Portfolio.py", f"Invalid transaction skipped: {t}")
            continue

        trade_type = t['trade_type'].lower()

        if trade_type == 'buy':
            fifo_queue.append({'quantity': quantity, 'price': price})
            total_quantity += quantity
            total_cost += quantity * price
        elif trade_type == 'sell':
            remaining = quantity
            while remaining > 0 and fifo_queue:
                buy = fifo_queue[0]
                matched_quantity = min(remaining, buy['quantity'])
                gain = matched_quantity * (price - buy['price'])
                realized_gain += gain

                buy['quantity'] -= matched_quantity
                remaining -= matched_quantity
                total_quantity -= matched_quantity
                total_cost -= matched_quantity * buy['price']

                if buy['quantity'] == 0:
                    fifo_queue.popleft()

    avg_cost = safe_divide(total_cost, total_quantity) if total_quantity > 0 else Decimal('0')
    return {
        'avg_cost': float(avg_cost),
        'position_size': float(total_quantity),
        'total_cost': float(total_cost),
        'realized_gain': float(realized_gain)
    }

# ------------------ DB Fetch Functions ------------------
def fetch_all_user_ids():
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT created_by FROM he_stock_transaction")
        user_ids = [row[0] for row in cursor.fetchall()]
        cursor.close()
        conn.close()
        return user_ids
    except Exception as err:
        log_error_to_db("HE_Portfolio.py", str(err))
        return []

def fetch_fifo_data(created_by):
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT ticker, date, trade_type, quantity, price, platform, created_by
            FROM he_stock_transaction
            WHERE created_by = %s
            ORDER BY date ASC
        """, (created_by,))
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        return rows
    except Exception as err:
        log_error_to_db("HE_Portfolio.py", str(err))
        return []

# ------------------ Market Index Data ------------------
def get_index_close(ticker_symbol):
    try:
        index = yf.Ticker(ticker_symbol)
        hist = index.history(period="1y", interval="1d", auto_adjust=True)
        return hist['Close'].iloc[-1] if not hist.empty else None
    except Exception as e:
        log_error_to_db("HE_Portfolio.py", f"{ticker_symbol}: {e}")
        return None

# ------------------ Summary Builder ------------------
def build_summary(rows):
    grouped = defaultdict(list)
    summary_list = []

    sp500_close = get_index_close("^GSPC")
    nasdaq_close = get_index_close("^IXIC")
    russell1000_close = get_index_close("^RUI")

    for row in rows:
        ticker, date, trade_type, quantity, price, platform, created_by = row
        grouped[(ticker, created_by)].append({
            'date': date,
            'trade_type': trade_type,
            'quantity': quantity,
            'price': price,
            'platform': platform
        })

    for (ticker, created_by), txns in grouped.items():
        txns_sorted = sorted(txns, key=lambda x: x['date'])
        fifo_result = process_fifo(txns_sorted)

        try:
            ticker_obj = yf.Ticker(ticker)
            info = ticker_obj.info
            hist = ticker_obj.history(period="1y", interval="1d", auto_adjust=True)

            current_price = safe_info_value(info, 'currentPrice', info.get('regularMarketPrice', 0))
            ema_50 = hist['Close'].ewm(span=50).mean().iloc[-1] if not hist.empty else None
            ema_100 = hist['Close'].ewm(span=100).mean().iloc[-1] if not hist.empty else None
            ema_200 = hist['Close'].ewm(span=200).mean().iloc[-1] if not hist.empty else None

            unrealized_gain_loss = fifo_result['position_size'] * (current_price - fifo_result['avg_cost']) if fifo_result['position_size'] > 0 else 0
            realized_gain_loss = fifo_result['realized_gain']

            first_buy_date = txns_sorted[0]['date']
            age_days = (datetime.now().date() - first_buy_date).days

            summary = {
                'ticker': ticker,
                'quantity': fifo_result['position_size'],
                'avg_cost': fifo_result['avg_cost'],
                'total_cost': fifo_result['total_cost'],
                'current_price': current_price,
                'position_size': fifo_result['position_size'],
                'unrealized_gain_loss': unrealized_gain_loss,
                'realized_gain_loss': realized_gain_loss,
                'first_buy_age': first_buy_date.strftime('%Y-%m-%d'),
                'avg_age_days': age_days,
                'platform': txns_sorted[0]['platform'],
                'industry_pe': safe_info_value(info, 'forwardPE'),
                'current_pe': safe_info_value(info, 'trailingPE'),
                'price_sales_ratio': safe_info_value(info, 'priceToSalesTrailing12Months'),
                'price_book_ratio': safe_info_value(info, 'priceToBook'),
                '50_day_ema': ema_50,
                '100_day_ema': ema_100,
                '200_day_ema': ema_200,
                'sp_500_ya': sp500_close,
                'nasdaq_ya': nasdaq_close,
                'russell_1000_ya': russell1000_close,
                'created_by': created_by,
                'peg_ratio': safe_info_value(info, 'pegRatio'),
                'net_profit_margin': safe_info_value(info, 'netMargins'),
                'fcf_yield': safe_round(safe_divide(info.get('freeCashflow', 0), info.get('marketCap', 1)) * 100),
                'pe_ratio': safe_info_value(info, 'trailingPE'),
                'roe': safe_info_value(info, 'returnOnEquity'),
                'current_ratio': safe_info_value(info, 'currentRatio'),
                'debt_equity': safe_info_value(info, 'debtToEquity'),
                'revenue_growth': safe_round(info.get('revenueGrowth', 0) * 100),
                'earnings_accuracy': safe_round(info.get('earningsQuarterlyGrowth', 0) * 100),
                'category': info.get('sector', 'N/A')
            }

            summary_list.append(summary)

        except Exception as e:
            log_error_to_db("HE_Portfolio.py", f"{ticker}: {e}")

    return pd.DataFrame(summary_list)

# ------------------ DB Insertion ------------------
def insert_summary_to_db(df):
    try:
        conn = get_connection()
        cursor = conn.cursor()

        for _, row in df.iterrows():
            cursor.execute("""
                INSERT INTO he_portfolio_master (
                    ticker, quantity, avg_cost, total_cost, current_price, position_size,
                    unrealized_gain_loss, realized_gain_loss, first_buy_age, avg_age_days,
                    platform, industry_pe, current_pe, price_sales_ratio, price_book_ratio,
                    50_day_ema, 100_day_ema, 200_day_ema, sp_500_ya, nasdaq_ya, russell_1000_ya,
                    created_by, pe_ratio, peg_ratio, roe, net_profit_margin, current_ratio,
                    debt_equity, fcf_yield, revenue_growth, earnings_accuracy, category
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                          %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, tuple(row))

        conn.commit()
        cursor.close()
        conn.close()
        print("✅ Portfolio summary inserted.")
    except Exception as err:
        log_error_to_db("HE_Portfolio.py", str(err))

# ------------------ Main ------------------
def main():
    user_ids = fetch_all_user_ids()
    if not user_ids:
        print("❌ No users found.")
        return

    for user_id in user_ids:
        print(f"\n📥 Processing user: {user_id}")
        rows = fetch_fifo_data(user_id)

        if not rows:
            print(f"⚠️ No transactions for user {user_id}")
            continue

        df = build_summary(rows)
        if not df.empty:
            print(tabulate(df, headers='keys', tablefmt='fancy_grid', showindex=False))
            insert_summary_to_db(df)
        else:
            print(f"⚠️ No data to insert for user {user_id}")

if __name__ == "__main__":
    main()
