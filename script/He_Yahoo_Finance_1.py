import yfinance as yf
import pandas as pd
import numpy as np
from tabulate import tabulate
import mysql.connector
from HE_Database_Connect import get_connection # Assuming this exists and works
import os
import traceback
from HE_Error_Logs import log_error_to_db 

# print(dir(yf)) # You can keep or remove this, it's just for debugging yfinance methods

def get_stock_data(symbol: str):
    try:
        stock = yf.Ticker(symbol)
        balance_sheet = stock.balance_sheet
        income_stmt = stock.financials
        df = stock.history(period="3mo")

        if df.empty:
            print(f"No stock data available for {symbol}. Check symbol or network.")
            return [None] * 28

        closing_prices = df["Close"].dropna().tolist()[-15:]
        current_price = closing_prices[-1] if closing_prices else None

        stock_info = stock.info
        eps = stock_info.get("trailingEps")
        bvps = stock_info.get("bookValue")
        revenue_ttm = stock_info.get("totalRevenue")
        market_cap = stock_info.get("marketCap")
        growth_rate = stock_info.get("earningsGrowth")
        ebitda = stock_info.get("ebitda")
        enterprise_value = stock_info.get("enterpriseValue")
        # Note: cogs is fetched twice, once here and once later from income_stmt.
        # It's better to fetch it once consistently from income_stmt if that's the primary source.
        cogs_info = stock_info.get("costOfRevenue") # This might be less reliable than income_stmt
        operating_income_info = stock_info.get("operatingIncome")
        total_assets_info = stock_info.get("totalAssets")
        net_income_info = stock_info.get("netIncome")
        total_equity_info = stock_info.get("totalStockholderEquity")

        # Prioritize data from financial statements for accuracy
        total_debt = balance_sheet.loc["Total Debt"].iloc[0] if "Total Debt" in balance_sheet.index else None
        # Assuming "Ordinary Shares Number" is equivalent to total equity for de_ratio calculation.
        # If it's number of shares, you might need market cap / share price for equity value.
        # Re-check what 'equity' should represent for your de_ratio.
        equity = balance_sheet.loc["Ordinary Shares Number"].iloc[0] if "Ordinary Shares Number" in balance_sheet.index else None

        ebit = income_stmt.loc["Operating Income"].iloc[0] if "Operating Income" in income_stmt.index else None
        interest_expense = income_stmt.loc["Interest Expense"].iloc[0] if "Interest Expense" in income_stmt.index else None
        revenue = income_stmt.loc["Total Revenue"].iloc[0] if "Total Revenue" in income_stmt.index else None
        total_assets_current = balance_sheet.loc["Total Assets"].iloc[0] if "Total Assets" in balance_sheet.index else None
        total_assets_previous = balance_sheet.loc["Total Assets"].iloc[1] if "Total Assets" in balance_sheet.index and balance_sheet.shape[1] > 1 else None
        cogs = income_stmt.loc["Cost Of Revenue"].iloc[0] if "Cost Of Revenue" in income_stmt.index else None
        inventory_current = balance_sheet.loc["Inventory"].iloc[0] if "Inventory" in balance_sheet.index else None
        inventory_previous = balance_sheet.loc["Inventory"].iloc[1] if "Inventory" in balance_sheet.index and balance_sheet.shape[1] > 1 else None

        net_income_keys = ["Net Income", "Net Income Applicable To Common Shares"]
        equity_keys = ["Total Stockholder Equity", "Common Stock Equity"]
        ar_keys = ["Net Receivables", "Accounts Receivable"]

        accounts_receivable = None
        for key in ar_keys:
            if key in balance_sheet.index:
                accounts_receivable = balance_sheet.loc[key].iloc[0]
                break

        inst_ownership = stock.info.get("heldPercentInstitutions", None)
        insider_ownership = stock.info.get("heldPercentInsiders", None)
        cash_flow = stock.cashflow

        # Consolidate fetching of common financial statement items if not already done
        if operating_income_info is None:
            operating_income = ebit # ebit is operating income
        else:
            operating_income = operating_income_info

        if net_income_info is None:
            net_income = next((income_stmt.loc[key].iloc[0] for key in net_income_keys if key in income_stmt.index), None)
        else:
            net_income = net_income_info

        if total_assets_info is None:
            total_assets = total_assets_current
        else:
            total_assets = total_assets_info

        if total_equity_info is None:
            total_equity = next((balance_sheet.loc[key].iloc[0] for key in equity_keys if key in balance_sheet.index), None)
        else:
            total_equity = total_equity_info

        # EPS Growth
        eps_previous = stock_info.get("forwardEps") # Often forwardEps is future, not previous.
                                                     # For historical EPS growth, you'd typically look at
                                                     # historical income statements.
                                                     # Adjust if this is not the intended calculation.
        eps_growth = None
        if eps and eps_previous and eps_previous != 0:
            eps_growth = ((eps - eps_previous) / eps_previous) * 100
            eps_growth = f"{round(eps_growth, 2):.2f}"

        # YOY Revenue Growth
        yoy_growth = None
        try:
            revenue_current = income_stmt.loc["Total Revenue"].iloc[0]
            revenue_previous = income_stmt.loc["Total Revenue"].iloc[1]
            if revenue_previous and revenue_previous != 0:
                yoy_growth = ((revenue_current - revenue_previous) / revenue_previous) * 100
                yoy_growth = f"{round(yoy_growth, 2):.2f}"
        except Exception:
            pass # yoy_growth remains None if data is unavailable

        # Current Ratio
        current_assets = None
        for asset_key in ["Total Current Assets", "Current Assets"]:
            if asset_key in balance_sheet.index:
                current_assets = balance_sheet.loc[asset_key].iloc[0]
                break

        current_liabilities = None
        for liability_key in ["Total Current Liabilities", "Current Liabilities"]:
            if liability_key in balance_sheet.index:
                current_liabilities = balance_sheet.loc[liability_key].iloc[0]
                break

        current_ratio = "Data Unavailable"
        if current_assets is not None and current_liabilities is not None and current_liabilities != 0:
            current_ratio_val = current_assets / current_liabilities
            current_ratio = f"{round(current_ratio_val, 2):.2f}"

        # Quick Ratio
        inventory = next((balance_sheet.loc[key].iloc[0] for key in ["Inventory"] if key in balance_sheet.index), 0)
        quick_ratio = None
        if current_assets is not None and inventory is not None and current_liabilities is not None and current_liabilities != 0:
            quick_ratio_val = (current_assets - inventory) / current_liabilities
            quick_ratio = f"{round(quick_ratio_val, 2):.2f}"

        # Debt-to-Equity Ratio
        de_ratio = None
        # Ensure 'equity' for D/E ratio is Total Stockholder Equity, not just Ordinary Shares Number
        # If 'Ordinary Shares Number' gives total equity value, then it's fine.
        # Otherwise, you might need: total_equity = next((balance_sheet.loc[key].iloc[0] for key in equity_keys if key in balance_sheet.index), None)
        if total_debt is not None and total_equity is not None and total_equity != 0:
            de_ratio_val = total_debt / total_equity
            de_ratio = f"{round(de_ratio_val, 2):.2f}"


        # Interest Coverage Ratio
        icr = None
        if ebit is not None and interest_expense is not None and interest_expense != 0:
            icr_val = ebit / interest_expense
            icr = f"{round(icr_val, 2):.2f}"

        # Asset Turnover Ratio
        asset_turnover = None
        if revenue is not None and total_assets_current is not None and total_assets_previous is not None:
            average_assets = (total_assets_current + total_assets_previous) / 2
            if average_assets != 0:
                asset_turnover_val = revenue / average_assets
                asset_turnover = f"{round(asset_turnover_val, 2):.2f}"

        # Inventory Turnover Ratio
        inventory_turnover = None
        if cogs is not None and inventory_current is not None and inventory_previous is not None:
            average_inventory = (inventory_current + inventory_previous) / 2
            if average_inventory != 0:
                inventory_turnover_val = cogs / average_inventory
                inventory_turnover = f"{round(inventory_turnover_val, 2):.2f}"

        # DSO (Days Sales Outstanding)
        dso = None
        if accounts_receivable is not None and revenue is not None and revenue != 0:
            dso_val = (accounts_receivable * 365) / revenue
            dso = f"{round(dso_val, 2):.2f}"

        # ROE (Return on Equity)
        roe = None
        if net_income is not None and total_equity is not None and total_equity != 0:
            roe_val = (net_income / total_equity) * 100
            roe = f"{round(roe_val, 2):.2f}"

        # Ownership Percentages
        if insider_ownership is not None:
            insider_ownership = insider_ownership * 100
            insider_ownership = f"{round(insider_ownership, 2):.2f}%"

        if inst_ownership is not None:
            inst_ownership = inst_ownership * 100
            inst_ownership = f"{round(inst_ownership, 2):.2f}%"

        # Operating Cash Flow
        operating_cash_flow = cash_flow.loc["Cash Flow From Continuing Operating Activities"].iloc[0] if "Cash Flow From Continuing Operating Activities" in cash_flow.index else None
        if operating_cash_flow is not None:
            operating_cash_flow = f"{round(operating_cash_flow, 2):.2f}"


        return df, closing_prices, current_price, eps, bvps, revenue_ttm, market_cap, growth_rate, ebitda, enterprise_value, cogs, net_income, operating_income, total_assets, total_equity, roe, eps_growth, yoy_growth, operating_cash_flow, current_ratio, quick_ratio, de_ratio, icr, asset_turnover, inventory_turnover, dso, insider_ownership, inst_ownership
    except Exception as e:
        error_message = traceback.format_exc()
        log_error_to_db(
            file_name=os.path.basename(__file__),
            error_description=error_message,
            created_by="Admin",
            env="dev"
        )
        print(f"Error fetching stock data for {symbol}: {e}")
        return [None] * 28


# Financial Ratio Calculation Functions (No changes needed, they are already defined)
def calculate_pe_ratio(price, eps):
    return round(price / eps, 2) if eps and eps > 0 else None

def calculate_pb_ratio(price, bvps):
    return round(price / bvps, 2) if bvps and bvps > 0 else None

def calculate_ps_ratio(market_cap, revenue_ttm):
    return round(market_cap / revenue_ttm, 2) if market_cap and revenue_ttm and revenue_ttm > 0 else None

def calculate_peg_ratio(pe_ratio, growth_rate):
    # Ensure growth_rate is not 0 for PEG calculation
    return round(pe_ratio / (growth_rate * 100), 2) if pe_ratio and growth_rate and growth_rate > 0 else None

def calculate_ev_ebitda(enterprise_value, ebitda):
    return round(enterprise_value / ebitda, 2) if enterprise_value and ebitda and ebitda > 0 else None

def calculate_gross_margin(revenue, cogs):
    if revenue is None or revenue <= 0:
        return "N/A"
    # The original code had an "estimated_cogs" if cogs is None.
    # It's better to return "N/A" if cogs is truly missing for accurate margin.
    if cogs is None:
        return "N/A"
    return round(((revenue - cogs) / revenue) * 100, 2)

def calculate_net_profit_margin(revenue, net_income):
    if revenue is None or revenue <= 0 or net_income is None:
        return "N/A"
    return round((net_income / revenue) * 100, 2)

def get_operating_margin(revenue, operating_income):
    return round((operating_income / revenue) * 100, 2) if revenue and operating_income and revenue > 0 else None

def calculate_roa(net_income, total_assets):
    return round((net_income / total_assets) * 100, 2) if net_income and total_assets and total_assets > 0 else None

# Technical Indicator Calculation Functions (No changes needed)
def calculate_sma(stock_prices):
    return round(sum(stock_prices) / len(stock_prices), 2) if stock_prices else None

def calculate_macd(df):
    if df.empty or len(df) < 26: # Need enough data points for EMAs
        return None, None
    df["Short EMA"] = df["Close"].ewm(span=12, adjust=False).mean()
    df["Long EMA"] = df["Close"].ewm(span=26, adjust=False).mean()
    df["MACD"] = df["Short EMA"] - df["Long EMA"]
    df["Signal"] = df["MACD"].ewm(span=9, adjust=False).mean()
    return round(df["MACD"].iloc[-1], 2), round(df["Signal"].iloc[-1], 2)

def calculate_adx(df):
    if df.shape[0] < 28: # ADX typically requires 14 periods for DM and another 14 for ADX, so at least 28-29 bars
        return None

    df_adx = df.copy() # Work on a copy to avoid SettingWithCopyWarning
    df_adx['tr'] = df_adx['High'].combine(df_adx['Close'].shift(), max) - df_adx['Low'].combine(df_adx['Close'].shift(), min)
    df_adx['atr'] = df_adx['tr'].rolling(window=14).mean()

    df_adx['up_move'] = df_adx['High'].diff()
    df_adx['down_move'] = df_adx['Low'].diff()

    # Calculate +DM and -DM
    df_adx['plus_dm'] = np.where((df_adx['up_move'] > df_adx['down_move']) & (df_adx['up_move'] > 0), df_adx['up_move'], 0)
    df_adx['minus_dm'] = np.where((df_adx['down_move'] > df_adx['up_move']) & (df_adx['down_move'] > 0), df_adx['down_move'], 0)

    # Calculate smoothed +DM and -DM (often done with Wilder's Smoothing, but simple MA is also common)
    # For ADX, often an EMA-like smoothing is used for +DI/-DI, which is the 14-period average
    df_adx['plus_di'] = 100 * (df_adx['plus_dm'].ewm(span=14, adjust=False).mean() / df_adx['atr'])
    df_adx['minus_di'] = 100 * (df_adx['minus_dm'].ewm(span=14, adjust=False).mean() / df_adx['atr'])
    
    # Calculate DX
    df_adx['dx'] = 100 * abs(df_adx['plus_di'] - df_adx['minus_di']) / (df_adx['plus_di'] + df_adx['minus_di'])
    
    # Calculate ADX (smoothed DX)
    adx = df_adx['dx'].ewm(span=14, adjust=False).mean()
    
    return round(adx.dropna().iloc[-1], 2) if not adx.dropna().empty else None


# Store data in MySQL database (No changes needed)
def store_data_in_db(data):
    conn = get_connection()
    if conn is None:
        print(" Could not connect to the database.")
        return

    try:
        cursor = conn.cursor()

        sql = """INSERT INTO stocks (symbol, latest_price, sma, macd, signal_macd, adx, pe_ratio, pb_ratio, ps_ratio, peg_ratio, ev_ebitda, gross_margin, net_margin, op_margin, roa, roe, eps_growth, yoy_growth, operating_cash_flow, current_ratio, quick_ratio, de_ratio, icr, asset_turnover, inventory_turnover, dso, insider_ownership, inst_ownership)
                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                 ON DUPLICATE KEY UPDATE
                    latest_price = VALUES(latest_price),
                    sma = VALUES(sma),
                    macd = VALUES(macd),
                    signal_macd = VALUES(signal_macd),
                    adx = VALUES(adx),
                    pe_ratio = VALUES(pe_ratio),
                    pb_ratio = VALUES(pb_ratio),
                    ps_ratio = VALUES(ps_ratio),
                    peg_ratio = VALUES(peg_ratio),
                    ev_ebitda = VALUES(ev_ebitda),
                    gross_margin = VALUES(gross_margin),
                    net_margin = VALUES(net_margin),
                    op_margin = VALUES(op_margin),
                    roa = VALUES(roa),
                    roe = VALUES(roe),
                    eps_growth = VALUES(eps_growth),
                    yoy_growth = VALUES(yoy_growth),
                    operating_cash_flow = VALUES(operating_cash_flow),
                    current_ratio = VALUES(current_ratio),
                    quick_ratio = VALUES(quick_ratio),
                    de_ratio = VALUES(de_ratio),
                    icr = VALUES(icr),
                    asset_turnover = VALUES(asset_turnover),
                    inventory_turnover = VALUES(inventory_turnover),
                    dso = VALUES(dso),
                    insider_ownership = VALUES(insider_ownership),
                    inst_ownership = VALUES(inst_ownership)"""

        converted_data = []
        for row in data:
            converted_row = tuple(
                float(x) if isinstance(x, (np.float64, np.float32)) else
                int(x) if isinstance(x, (np.int64, np.int32)) else x
                for x in row
            )
            converted_data.append(converted_row)

        cursor.executemany(sql, converted_data)
        conn.commit()
        print(" Data stored successfully!")
    except mysql.connector.Error as err:
        print(f" Database Error: {err}")
        # Log this database error using your logging function as well
        log_error_to_db(
            file_name=os.path.basename(__file__),
            error_description=f"Database insert error: {err}",
            created_by="admin",
            env="dev"
        )
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()

if __name__ == "__main__":
    stock_symbols = ["AAPL", "MSFT", "GOOGL", "CAVA", "AMZN","TSLA","TMDX"]
    results = []

    for symbol in stock_symbols:
        # Unpack all 28 expected return values
        (df, stock_prices, current_price, eps, bvps, revenue_ttm, market_cap, growth_rate,
         ebitda, enterprise_value, cogs, net_income, operating_income, total_assets,
         total_equity, roe, eps_growth, yoy_growth, operating_cash_flow, current_ratio,
         quick_ratio, de_ratio, icr, asset_turnover, inventory_turnover, dso,
         insider_ownership, inst_ownership) = get_stock_data(symbol)

        # Ensure all required values are not None before calculating ratios and appending
        if (df is not None and stock_prices and current_price is not None):
            macd_val, signal_val = calculate_macd(df)
            adx_val = calculate_adx(df)
            pe_ratio_val = calculate_pe_ratio(current_price, eps)
            pb_ratio_val = calculate_pb_ratio(current_price, bvps)
            ps_ratio_val = calculate_ps_ratio(market_cap, revenue_ttm)
            # Ensure pe_ratio_val and growth_rate are not None for PEG
            peg_ratio_val = calculate_peg_ratio(pe_ratio_val, growth_rate)
            ev_ebitda_val = calculate_ev_ebitda(enterprise_value, ebitda)
            gross_margin_val = calculate_gross_margin(revenue_ttm, cogs)
            net_profit_margin_val = calculate_net_profit_margin(revenue_ttm, net_income)
            operating_margin_val = get_operating_margin(revenue_ttm, operating_income)
            roa_val = calculate_roa(net_income, total_assets)


            results.append((symbol, current_price, calculate_sma(stock_prices), macd_val, signal_val,
                            adx_val, pe_ratio_val, pb_ratio_val, ps_ratio_val, peg_ratio_val,
                            ev_ebitda_val, gross_margin_val, net_profit_margin_val, operating_margin_val,
                            roa_val, roe, eps_growth, yoy_growth, operating_cash_flow, current_ratio,
                            quick_ratio, de_ratio, icr, asset_turnover, inventory_turnover, dso,
                            insider_ownership, inst_ownership))
        else:
            print(f"Skipping {symbol} due to missing core data.")

    store_data_in_db(results)
    
    # Prepare data for tabulation, converting Nones to "N/A" for display
    display_results = []
    for row in results:
        display_row = [val if val is not None else "N/A" for val in row]
        display_results.append(display_row)

    print(tabulate(display_results, headers=["Symbol", "Price", "SMA", "MACD", "Signal", "ADX", "P/E", "P/B", "P/S", "PEG", "EV/EBITDA", "Gross M", "Net M", "Op M", "ROA", "ROE","EPS Growth","YOY Growth","Operating Cash Flow","Current Ratio","Quick Ratio","D/E Ratio","ICR","Asset Turnover","Inventory Turnover","DSO","Insider Ownership %","Institutional Ownership %"], tablefmt="grid"))