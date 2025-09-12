import yfinance as yf
import numpy as np
from HE_Database_Connect import get_connection
from HE_Error_Logs import log_error_to_db 


indices = {
    "Dow Jones": "^DJI",
    "Russell 2000": "^RUT",
    "S&P 500": "^GSPC",
    "CBOE Volatility Index": "^VIX"
}

def fetch_index_data(symbol):
    try:
        ticker = yf.Ticker(symbol)
        data = ticker.history(period="2d")
        data = data.dropna(subset=["Open", "Close"])

        if data.empty:
            print(f"📭 No valid data returned for {symbol}")
            return None, None

        open_price = data['Open'].iloc[-1]
        close_price = data['Close'].iloc[-1]

        if open_price == 0:
            print(f" Open price is zero for {symbol}")
            return None, None

        percent_change = ((close_price - open_price) / open_price) * 100

        if np.isnan(percent_change) or np.isinf(percent_change):
            print(f" Invalid percent change for {symbol}")
            return None, None

        return float(round(close_price, 2)), float(round(percent_change, 2))

    except Exception as e:
        print(f" Error fetching data for {symbol}: {e}")
        log_error_to_db("he_symbol_close_price.py", str(e), created_by="fetch_index_data")
        return None, None

def create_table_if_not_exists(cursor):
    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS he_index_data (
                symbol VARCHAR(10) PRIMARY KEY,
                index_name VARCHAR(50),
                close_price DECIMAL(10,2),
                percent_change DECIMAL(6,2)
            )
        """)
    except Exception as e:
        print(f" Error creating table: {e}")
        log_error_to_db("he_symbol_close_price.py", str(e), created_by="create_table_if_not_exists")

def store_index_data():
    try:
        conn = get_connection()
        cursor = conn.cursor()

        create_table_if_not_exists(cursor)

        for name, symbol in indices.items():
            close_price, percent_change = fetch_index_data(symbol)

            if close_price is not None:
                try:
                    cursor.execute("""
                        INSERT INTO he_index_data (index_name, symbol, close_price, percent_change)
                        VALUES (%s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                            index_name = VALUES(index_name),
                            close_price = VALUES(close_price),
                            percent_change = VALUES(percent_change)
                    """, (name, symbol, close_price, percent_change))

                    print(f" Inserted: {name} ({symbol}) → Close: {close_price}, Change: {percent_change}%")
                except Exception as insert_err:
                    print(f" Insert error for {symbol}: {insert_err}")
                    log_error_to_db("index_data_store.py", str(insert_err), created_by="store_index_data - insert")
            else:
                print(f" Skipped: {name} ({symbol}) – Invalid or missing data")

        conn.commit()
        print(" All data committed to the database.")

    except Exception as e:
        print(f" Error storing index data: {e}")
        log_error_to_db("he_symbol_close_price.py", str(e), created_by="store_index_data")

    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    store_index_data()
