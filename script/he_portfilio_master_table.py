import pandas as pd
from datetime import datetime
from collections import deque, defaultdict
from decimal import Decimal, InvalidOperation
import yfinance as yf
from tabulate import tabulate
import mysql.connector
import math


# ---------- Utility Functions ----------

def safe_round(val, digits=2):
    try:
        return round(float(val), digits)
    except (ValueError, TypeError, InvalidOperation):
        return 0

def clean_dataframe(df):
    for col in df.columns:
        df[col] = df[col].apply(lambda x: None if isinstance(x, float) and (math.isinf(x) or math.isnan(x)) else x)
    return df

def fetch_fifo_data():
    try:
        conn = mysql.connector.connect(
            host="localhost", user="root", password="123", database="hitman_edgev_1"
        )
        cursor = conn.cursor()
        cursor.execute("""
            SELECT ticker, date, trade_type, quantity, price, platform, created_by
            FROM he_stock_transaction;
        """)
        rows = cursor.fetchall()
        cursor.close()
        conn.close()

        print("\nFetched Transactions:")
        print(tabulate(rows, headers=["Ticker", "Date", "Type", "Qty", "Price", "Platform", "Created By"], tablefmt="grid"))
        return rows

    except mysql.connector.Error as err:
        print(f"Database Error: {err}")
        return []

def safe_get(df, keys):
    for key in keys:
        if key in df.index:
            return df.loc[key].iloc[0]
    return None

def get_index_return(ticker):
    try:
        hist = yf.Ticker(ticker).history(period="1y")
        if hist.empty:
            return None
        start_price = hist['Close'].iloc[0]
        end_price = hist['Close'].iloc[-1]
        return round((end_price - start_price) / start_price * 100, 2)
    except:
        return None

# ---------- Index Returns ----------

sp500_return = get_index_return("^GSPC")
nasdaq_return = get_index_return("^IXIC")
russell1000_return = get_index_return("^RUI")

grouped = defaultdict(list)
platform_map = {}

# ---------- Fetch Transactions and Group ----------

for t in fetch_fifo_data():
    if len(t) != 7:
        print(f"Skipping invalid row (length != 7): {t}")
        continue

    ticker, date_obj, action, qty, price, platform, created_by = t

    ticker = ticker or "UNKNOWN"
    platform = platform or "UNKNOWN"
    action = (action or "unknown").strip().lower()

    try:
        qty = Decimal(qty) if qty is not None else Decimal('0')
    except (InvalidOperation, TypeError):
        print(f"⚠️ Invalid quantity for row: {t}")
        qty = Decimal('0')

    try:
        price = Decimal(price) if price is not None else Decimal('0')
    except (InvalidOperation, TypeError):
        print(f"⚠️ Invalid price for row: {t}")
        price = Decimal('0')

    if not date_obj:
        print(f"⚠️ Missing date for row: {t}, using today's date")
        date_str = datetime.today().strftime("%Y-%m-%d")
    else:
        date_str = date_obj.strftime("%Y-%m-%d")

    grouped[ticker].append((date_str, ticker, action, qty, price, platform, created_by))
    platform_map[ticker] = platform

# ---------- Process Each Ticker ----------

summary_list = []

for ticker, txns in grouped.items():
    print(f"\n📊 Processing: {ticker}")
    holdings = deque()
    cumulative_buy_cost = Decimal('0')
    total_qty = Decimal('0')
    realized_gain_loss = Decimal('0')
    first_buy_date = None

    try:
        stock = yf.Ticker(ticker)
        hist = stock.history(period="260d")
        if hist.empty or 'Close' not in hist:
            print(f"❌ Skipping {ticker} — No valid historical data.")
            continue
    except Exception as e:
        print(f"❌ Skipping {ticker} — Error fetching history: {e}")
        continue

    ema_50 = safe_round(hist['Close'].ewm(span=50, adjust=False).mean().iloc[-1])
    ema_100 = safe_round(hist['Close'].ewm(span=100, adjust=False).mean().iloc[-1])
    ema_200 = safe_round(hist['Close'].ewm(span=200, adjust=False).mean().iloc[-1])

    try:
        info = stock.info
        current_price = Decimal(info.get('currentPrice', 0))
        category = info.get('sector', 'Unknown')
    except Exception as e:
        print(f"⚠️ Failed to fetch info for {ticker}: {e}")
        current_price = Decimal('0')
        category = "Unknown"
        info = {}

    for date_str, symbol, action, qty, price, platform, created_by in txns:
        try:
            date = datetime.strptime(date_str, "%Y-%m-%d")
        except Exception as e:
            print(f"Invalid date format: {date_str} in {symbol} — {e}")
            continue

        if action == 'buy':
            if not first_buy_date:
                first_buy_date = date
            holdings.append([qty, price, date])
            total_qty += qty
            cumulative_buy_cost += qty * price

        elif action == 'sell':
            sell_qty = qty
            while sell_qty > 0 and holdings:
                h_qty, h_price, h_date = holdings[0]
                used_qty = min(sell_qty, h_qty)
                profit = (price - h_price) * used_qty
                realized_gain_loss += profit
                cumulative_buy_cost -= used_qty * h_price
                total_qty -= used_qty
                if used_qty == h_qty:
                    holdings.popleft()
                else:
                    holdings[0][0] -= used_qty
                sell_qty -= used_qty

    avg_cost = cumulative_buy_cost / total_qty if total_qty else Decimal('0')
    total_cost = cumulative_buy_cost
    unrealized = (current_price - avg_cost) * total_qty if total_qty else Decimal('0')

    today = datetime.today()
    first_buy_age = (today - first_buy_date).days if first_buy_date else "-"
    average_age = (
        sum((today - h[2]).days * float(h[0]) for h in holdings) / float(total_qty)
        if total_qty > 0 else "-"
    )

    try:
        balance_sheet = stock.balance_sheet
        income_stmt = stock.financials
        cashflow_stmt = stock.cashflow

        net_income = safe_get(income_stmt, ["Net Income", "Net Income Applicable To Common Shares"])
        equity = safe_get(balance_sheet, ["Total Stockholder Equity", "Common Stock Equity"])
        total_revenue = safe_get(income_stmt, ["Total Revenue"])
        current_assets = safe_get(balance_sheet, ["Total Current Assets", "Current Assets"])
        current_liabilities = safe_get(balance_sheet, ["Total Current Liabilities", "Current Liabilities"])
        inventory = safe_get(balance_sheet, ["Inventory", "Total Inventory"]) or 0
        total_debt = safe_get(balance_sheet, ["Total Debt", "Short Long Term Debt Total"])

        op_cashflow = next((cashflow_stmt.loc[row].iloc[0] for row in cashflow_stmt.index if "operating" in row.lower()), None)
        capex = next((cashflow_stmt.loc[row].iloc[0] for row in cashflow_stmt.index if "capital expenditure" in row.lower()), 0)
        fcf = (op_cashflow + capex) if op_cashflow else None

    except Exception as e:
        print(f"⚠️ Financial data missing for {ticker}: {e}")
        net_income = equity = total_revenue = current_assets = current_liabilities = total_debt = None
        inventory = 0
        fcf = None

    eps = info.get("trailingEps") or info.get("forwardEps")
    growth_rate = info.get("earningsQuarterlyGrowth", 0.12)
    fwd_rev_growth = info.get("revenueGrowth")
    surprise_pct = info.get("earningsQuarterlyGrowth")
    market_cap = info.get("marketCap")

    pe_ratio = safe_round(current_price / Decimal(eps)) if current_price and eps else None
    peg_ratio = safe_round(pe_ratio / (growth_rate * 100)) if pe_ratio and growth_rate else None
    roe = safe_round((net_income / equity) * 100) if net_income and equity else None
    current_ratio = safe_round(current_assets / current_liabilities) if current_assets and current_liabilities else None
    quick_ratio = safe_round((current_assets - inventory) / current_liabilities) if current_assets and current_liabilities else None
    de_ratio = safe_round(total_debt / equity) if total_debt and equity else None
    net_profit_margin = safe_round((net_income / total_revenue) * 100) if net_income and total_revenue else None
    fcf_yield = safe_round((fcf / market_cap) * 100) if fcf and market_cap else None

    summary_list.append({
        "ticker": ticker,
        "Category": category,
        "quantity": float(total_qty),
        "avg_cost": safe_round(avg_cost),
        "total_cost": safe_round(total_cost),
        "current_price": safe_round(current_price),
        "unrealized_gain_loss": safe_round(unrealized),
        "relized_gain_loss": safe_round(realized_gain_loss),
        "first_buy_age": first_buy_age,
        "avg_age_days": round(average_age, 1) if isinstance(average_age, float) else average_age,
        "platform": platform_map[ticker],
        "industry_pe": safe_round(info.get('trailingPE')),
        "current_pe": safe_round(info.get('forwardPE')),
        "price_sales_ratio": safe_round(info.get('priceToSalesTrailing12Months')),
        "price_book_ratio": safe_round(info.get('priceToBook')),
        "50_day_ema": ema_50,
        "100_day_ema": ema_100,
        "200_day_ema": ema_200,
        "sp_500_ya": sp500_return,
        "nashdaq_ya": nasdaq_return,
        "russel_1000_ya": russell1000_return,
        "pe_ratio": pe_ratio,
        "peg_ratio": peg_ratio,
        "roe": roe,
        "net_profit_margin": net_profit_margin,
        "current_ratio": current_ratio,
        "debt_equity": de_ratio,
        "fcf_yield": fcf_yield,
        "revenue_growth": safe_round(fwd_rev_growth * 100) if isinstance(fwd_rev_growth, (float, int)) else None,
        "earnings_accuracy": safe_round(surprise_pct * 100) if isinstance(surprise_pct, (float, int)) else None,
        "created_by": created_by
  

    })

# ---------- Create DataFrame and Insert ----------

df = pd.DataFrame(summary_list)

if not df.empty:
    df['position_size'] = (df['total_cost'] / df['total_cost'].sum()).round(2)

    print("\n📈 Portfolio Summary:")
    print(tabulate(df, headers="keys", tablefmt="grid"))

    # Fill any missing columns
    required_columns = [
        "ticker", "Category", "quantity", "avg_cost", "position_size", "total_cost", "current_price",
        "unrealized_gain_loss", "relized_gain_loss", "first_buy_age", "avg_age_days", "platform",
        "industry_pe", "current_pe", "price_sales_ratio", "price_book_ratio",
        "50_day_ema", "100_day_ema", "200_day_ema",
        "sp_500_ya", "nashdaq_ya", "russel_1000_ya",
        "pe_ratio", "peg_ratio", "roe", "net_profit_margin", "current_ratio", "debt_equity", "fcf_yield",
        "revenue_growth", "earnings_accuracy", "created_by"
    ]
    for col in required_columns:
        if col not in df.columns:
            df[col] = None

    # Clean any NaNs/inf from float columns
    df = clean_dataframe(df)

    try:
        conn = mysql.connector.connect(
            host="localhost", user="root", password="123", database="hitman_edgev_1"
        )
        cursor = conn.cursor()

        query = """
            INSERT INTO he_portfolio_master (
                ticker, Category, quantity, avg_cost, position_size, total_cost, current_price,
                unrealized_gain_loss, relized_gain_loss, first_buy_age, avg_age_days, platform,
                industry_pe, current_pe, price_sales_ratio, price_book_ratio,
                `50_day_ema`, `100_day_ema`, `200_day_ema`,
                sp_500_ya, nashdaq_ya, russel_1000_ya,
                pe_ratio, peg_ratio, roe, net_profit_margin, current_ratio, debt_equity, fcf_yield,
                revenue_growth, earnings_accuracy,
                created_by
            )
            VALUES (
                %(ticker)s, %(Category)s, %(quantity)s, %(avg_cost)s, %(position_size)s, %(total_cost)s, %(current_price)s,
                %(unrealized_gain_loss)s, %(relized_gain_loss)s, %(first_buy_age)s, %(avg_age_days)s, %(platform)s,
                %(industry_pe)s, %(current_pe)s, %(price_sales_ratio)s, %(price_book_ratio)s,
                %(50_day_ema)s, %(100_day_ema)s, %(200_day_ema)s,
                %(sp_500_ya)s, %(nashdaq_ya)s, %(russel_1000_ya)s,
                %(pe_ratio)s, %(peg_ratio)s, %(roe)s, %(net_profit_margin)s, %(current_ratio)s, %(debt_equity)s, %(fcf_yield)s,
                %(revenue_growth)s, %(earnings_accuracy)s,
                %(created_by)s
            )
        """

        cursor.executemany(query, df.to_dict(orient="records"))
        conn.commit()
        print("\n✅ Data inserted into `he_portfolio_master` successfully.")

    except mysql.connector.Error as err:
        print(f"\n❌ MySQL Insertion Error: {err}")

    finally:
        cursor.close()
        conn.close()
else:
    print("\n⚠️ No valid data available to insert.")
