from tabulate import tabulate
from HE_Database_Connect import get_connection
from HE_Error_Logs import log_error_to_db 
import pandas as pd
from collections import deque

def fetch_all_stock_data():
    try:
        conn = get_connection()
        cursor = conn.cursor()

        query = "SELECT * FROM stock_transactions"
        cursor.execute(query)

        columns = [desc[0] for desc in cursor.description]
        result = cursor.fetchall()

        cursor.close()
        conn.close()

        return columns, result

    except Exception as err:
        print(f"Error: {err}")
        log_error_to_db("investment_calculator.py", str(err), created_by="fetch_all_stock_data")
        return None, None

if __name__ == "__main__":
    columns, data = fetch_all_stock_data()
    if data:
        print("\n Stock Transactions:\n")
        print(tabulate(data, headers=columns, tablefmt="grid"))
    else:
        print(" No data found or DB error.\n")



class InvestmentCalculator:
    def __init__(self):
        self.transactions = {}
        try:
            self.fetch_stock_transactions()
        except Exception as e:
            print(f"[Init Error] {e}")
            log_error_to_db("he_summary.py", str(e), created_by="__init__")

    def fetch_stock_transactions(self):
        """Fetch and organize stock transactions grouped by instrument."""
        try:
            conn = get_connection()
            cursor = conn.cursor()

            query = """
            SELECT LOWER(instrument), tran_code, quantity, price, activity_date
            FROM stock_transactions
            ORDER BY activity_date ASC
            """
            cursor.execute(query)

            for instrument, tran_code, qty, price, date in cursor.fetchall():
                if instrument not in self.transactions:
                    self.transactions[instrument] = {"buy": deque(), "sell": []}

                if tran_code.lower() == "buy":
                    self.transactions[instrument]["buy"].append((qty, price, date))
                elif tran_code.lower() == "sell":
                    self.transactions[instrument]["sell"].append((qty, price, date))

            cursor.close()
            conn.close()

        except Exception as err:
            print(f"[Fetch Transactions Error] {err}")
            log_error_to_db("investment_calculator.py", str(err), created_by="fetch_stock_transactions")

    def calculate(self):
        table_data = []

        try:
            for instrument, data in self.transactions.items():
                buy_queue = data["buy"]
                total_qty = 0
                total_investment = 0

                
                for sell_qty, _, _ in data["sell"]:
                    while sell_qty > 0 and buy_queue:
                        buy_qty, buy_price, _ = buy_queue.popleft()
                        if buy_qty <= sell_qty:
                            sell_qty -= buy_qty
                        else:
                            buy_queue.appendleft((buy_qty - sell_qty, buy_price, _))
                            sell_qty = 0

               
                for qty, price, _ in buy_queue:
                    total_qty += qty
                    total_investment += qty * price

                avg_price = total_investment / total_qty if total_qty > 0 else 0

                table_data.append([instrument.upper(), round(total_investment, 2), total_qty, round(avg_price, 2)])

            df = pd.DataFrame(table_data, columns=["Instrument", "Total Investment", "Total Quantity", "Average Price"])
            df["Average Price"] = df["Average Price"].fillna(0)

            self.insert_data_into_db(df)
            return df

        except Exception as e:
            print(f"[Calculation Error] {e}")
            log_error_to_db("he_summary.py", str(e), created_by="calculate")
            return pd.DataFrame(columns=["Instrument", "Total Investment", "Total Quantity", "Average Price"])

    def insert_data_into_db(self, df):
        try:
            conn = get_connection()
            cursor = conn.cursor()

            for _, row in df.iterrows():
                query = """
                INSERT INTO summary (instrument, total_investment, total_quantity, average_price)
                VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    total_investment = VALUES(total_investment),
                    total_quantity = VALUES(total_quantity),
                    average_price = VALUES(average_price)
                """
                cursor.execute(query, (
                    row["Instrument"], row["Total Investment"],
                    row["Total Quantity"], row["Average Price"]
                ))

            conn.commit()
            cursor.close()
            conn.close()
            print("\n Summary data stored successfully.\n")

        except Exception as e:
            print(f"[Insert DB Error] {e}")
            log_error_to_db("he_summary.py", str(e), created_by="insert_data_into_db")


if __name__ == "__main__":
    try:
        calculator = InvestmentCalculator()
        result_df = calculator.calculate()

        print(" Investment Summary:\n")
        print(tabulate(result_df, headers="keys", tablefmt="grid", showindex=False))
    except Exception as e:
        log_error_to_db("he_summary.py", str(e), created_by="main")
        print("[Main Error] Program failed to execute.")
