import yfinance as yf
import pandas as pd
import numpy as np
from tabulate import tabulate
import traceback

from HE_Database_Connect import get_connection
from HE_Error_Logs import log_error_to_db 

def get_stock_data(symbol: str):
    try:
        stock = yf.Ticker(symbol)
        df = stock.history(period="3mo")
        if df.empty or "Close" not in df.columns:
            raise ValueError(f"No stock data available for {symbol}.")
        return df
    except Exception:
        log_error_to_db("he_option_trading_pull_matrics.py", traceback.format_exc(), created_by="data_fetch")
        return None

def calculate_macd(df):
    try:
        df["Short EMA"] = df["Close"].ewm(span=12, adjust=False).mean()
        df["Long EMA"] = df["Close"].ewm(span=26, adjust=False).mean()
        df["MACD"] = df["Short EMA"] - df["Long EMA"]
        df["Signal"] = df["MACD"].ewm(span=9, adjust=False).mean()
        return round(df["MACD"].iloc[-1], 2), round(df["Signal"].iloc[-1], 2)
    except Exception:
        log_error_to_db("he_option_trading_pull_matrics.py", traceback.format_exc(), created_by="macd_calc")
        return 0, 0

def calculate_bollinger_bands(df, period=20):
    try:
        df["SMA"] = df["Close"].rolling(window=period).mean()
        df["STD"] = df["Close"].rolling(window=period).std()
        df["Upper Band"] = df["SMA"] + (2 * df["STD"])
        df["Lower Band"] = df["SMA"] - (2 * df["STD"])
        return round(df["Upper Band"].iloc[-1], 2), round(df["Lower Band"].iloc[-1], 2)
    except Exception:
        log_error_to_db("he_option_trading_pull_matrics.py", traceback.format_exc(), created_by="bollinger_calc")
        return 0, 0

def calculate_sma(df, period=20):
    try:
        return round(df["Close"].rolling(window=period).mean().iloc[-1], 2)
    except Exception:
        log_error_to_db("he_option_trading_pull_matrics.py", traceback.format_exc(), created_by="sma_calc")
        return 0

def calculate_ema(df, period=20):
    try:
        return round(df["Close"].ewm(span=period, adjust=False).mean().iloc[-1], 2)
    except Exception:
        log_error_to_db("he_option_trading_pull_matrics.py", traceback.format_exc(), created_by="ema_calc")
        return 0

def calculate_fibonacci_levels(df):
    try:
        recent_high = df["High"].max()
        recent_low = df["Low"].min()
        diff = recent_high - recent_low
        return {
            "Fib 23.6%": round(recent_high - 0.236 * diff, 2),
            "Fib 38.2%": round(recent_high - 0.382 * diff, 2),
            "Fib 50.0%": round(recent_high - 0.500 * diff, 2),
            "Fib 61.8%": round(recent_high - 0.618 * diff, 2),
            "Fib 78.6%": round(recent_high - 0.786 * diff, 2),
        }
    except Exception:
        log_error_to_db("he_option_trading_pull_matrics.py", traceback.format_exc(), created_by="fibonacci_calc")
        return {k: 0 for k in ["Fib 23.6%", "Fib 38.2%", "Fib 50.0%", "Fib 61.8%", "Fib 78.6%"]}

def calculate_atr(df, period=14):
    try:
        df["H-L"] = df["High"] - df["Low"]
        df["H-PC"] = abs(df["High"] - df["Close"].shift(1))
        df["L-PC"] = abs(df["Low"] - df["Close"].shift(1))
        df["TR"] = df[["H-L", "H-PC", "L-PC"]].max(axis=1)
        df["ATR"] = df["TR"].rolling(window=period).mean()
        return round(df["ATR"].iloc[-1], 2)
    except Exception:
        log_error_to_db("he_option_trading_pull_matrics.py", traceback.format_exc(), created_by="atr_calc")
        return 0

def calculate_stochastic(df, k_period=14, d_period=3):
    try:
        df["Low_Min"] = df["Low"].rolling(window=k_period).min()
        df["High_Max"] = df["High"].rolling(window=k_period).max()
        df["%K"] = 100 * ((df["Close"] - df["Low_Min"]) / (df["High_Max"] - df["Low_Min"]))
        df["%D"] = df["%K"].rolling(window=d_period).mean()
        return round(df["%K"].iloc[-1], 2), round(df["%D"].iloc[-1], 2)
    except Exception:
        log_error_to_db("he_option_trading_pull_matrics.py", traceback.format_exc(), created_by="stochastic_calc")
        return 0, 0

def calculate_rsi(df, period=14):
    try:
        delta = df["Close"].diff()
        gain = delta.where(delta > 0, 0.0)
        loss = -delta.where(delta < 0, 0.0)
        avg_gain = gain.rolling(window=period).mean()
        avg_loss = loss.rolling(window=period).mean()
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        return round(rsi.iloc[-1], 2)
    except Exception:
        log_error_to_db("he_option_trading_pull_matrics.py", traceback.format_exc(), created_by="rsi_calc")
        return 0


def store_data_in_db(data):
    try:
        conn = get_connection()
        cursor = conn.cursor()
        sql = """INSERT INTO stocks (symbol, latest_price, macd, signal_macd, boll_upper, boll_lower,
                                            atr, volume, stoch_k, stoch_d, sma, ema,
                                            fib_23_6, fib_38_2, fib_50, fib_61, fib_78_6, rsi)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                            latest_price=VALUES(latest_price),
                            macd=VALUES(macd), signal_macd=VALUES(signal_macd),
                            boll_upper=VALUES(boll_upper), boll_lower=VALUES(boll_lower),
                            atr=VALUES(atr), volume=VALUES(volume),
                            stoch_k=VALUES(stoch_k), stoch_d=VALUES(stoch_d),
                            sma=VALUES(sma), ema=VALUES(ema),
                            fib_23_6=VALUES(fib_23_6), fib_38_2=VALUES(fib_38_2),
                            fib_50=VALUES(fib_50), fib_61=VALUES(fib_61),
                            fib_78_6=VALUES(fib_78_6), rsi=VALUES(rsi)"""
        
        converted_data = [
            tuple(float(x) if isinstance(x, (np.float64, np.float32)) else int(x) if isinstance(x, (np.int64, np.int32)) else x for x in row)
            for row in data
        ]

        cursor.executemany(sql, converted_data)
        conn.commit()
        cursor.close()
        conn.close()
        print(" Data stored successfully!")
    except Exception:
        log_error_to_db("he_option_trading_pull_matrics.py", traceback.format_exc(), created_by="db_store")
        print(" Database Error - Logged")

if __name__ == "__main__":
    stock_symbols = ["AAPL", "MSFT", "GOOGL", "CAVA", "AMZN", "TSLA", "TMDX"]
    results = []

    for symbol in stock_symbols:
        df = get_stock_data(symbol)
        if df is not None:
            try:
                macd, signal = calculate_macd(df)
                upper, lower = calculate_bollinger_bands(df)
                stochastic_k, stochastic_d = calculate_stochastic(df)
                atr = calculate_atr(df)
                current_price = round(df["Close"].iloc[-1], 2)
                latest_volume = int(df["Volume"].iloc[-1])
                sma = calculate_sma(df)
                ema = calculate_ema(df)
                fib_levels = calculate_fibonacci_levels(df)
                rsi = calculate_rsi(df)

                results.append([
                    symbol, current_price, macd, signal, upper, lower, atr, latest_volume,
                    stochastic_k, stochastic_d, sma, ema,
                    fib_levels["Fib 23.6%"], fib_levels["Fib 38.2%"], fib_levels["Fib 50.0%"],
                    fib_levels["Fib 61.8%"], fib_levels["Fib 78.6%"], rsi
                ])
            except Exception:
                log_error_to_db("he_option_trading_pull_matrics.py", traceback.format_exc(), created_by=f"{symbol}_loop")

    headers = [
        "Symbol", "Price", "MACD", "Signal",
        "Boll Upper", "Boll Lower", "ATR", "Volume",
        "Stoch %K", "Stoch %D", "SMA", "EMA",
        "Fib 23.6%", "Fib 38.2%", "Fib 50%", "Fib 61.8%", "Fib 78.6%", "RSI"
    ]

    print(tabulate(results, headers=headers, tablefmt="pretty"))
    store_data_in_db(results)
