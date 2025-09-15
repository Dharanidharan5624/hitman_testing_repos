from collections import deque
from tabulate import tabulate
import mysql.connector
import traceback
from HE_Database_Connect import get_connection
from HE_Error_Logs import log_error_to_db  

def process_buy(holdings, cumulative_buy_cost, one, balance_qty, date, ticker, buy_qty, price):
    total_cost = buy_qty * price
    holdings.append([buy_qty, price, buy_qty])  

    cumulative_buy_cost += total_cost
    one += buy_qty
    balance_qty += buy_qty

    avg_cost = cumulative_buy_cost / balance_qty if balance_qty > 0 else 0

    return holdings, cumulative_buy_cost, one, balance_qty, [
        date, ticker, "Buy", buy_qty, "", price, "", "", total_cost, "",
        cumulative_buy_cost, balance_qty, round(avg_cost, 2)
    ]

def process_sell(holdings, cumulative_buy_cost, one, balance_qty, date, ticker, sell_qty, price, sale_price):
    if not holdings or sell_qty <= 0:
        print("No holdings available or sell quantity is zero.")
        return holdings, cumulative_buy_cost, one, balance_qty, []

    realized_cost = 0
    total_sell_value = 0
    sell_profit = 0
    qty_to_sell = sell_qty

    while qty_to_sell > 0 and holdings:
        buy_qty, buy_price, bal_qty = holdings[0]
        qty_sold = min(qty_to_sell, bal_qty)

        realized_cost += qty_sold * buy_price
        sell_profit += (sale_price - buy_price) * qty_sold
        total_sell_value += qty_sold * sale_price

        if qty_sold == bal_qty:
            holdings.popleft()
        else:
            holdings[0][2] -= qty_sold

        qty_to_sell -= qty_sold

    cumulative_buy_cost -= realized_cost
    one -= sell_qty
    balance_qty -= sell_qty
    avg_cost = cumulative_buy_cost / balance_qty if balance_qty > 0 else 0

    return holdings, cumulative_buy_cost, one, balance_qty, [
        date, ticker, "Sell", sell_qty, one, price, sale_price, round(sell_profit, 2),
        realized_cost, round(total_sell_value, 2), cumulative_buy_cost, balance_qty, round(avg_cost, 2)
    ]

def fifo_tracker(transactions, cursor, db):
    holdings = deque()
    balance_qty = 0
    cumulative_buy_cost = 0
    one = 0
    transaction_results = []
    insert_queries = []

    for date, ticker, action, qty, price, *sale_price in transactions:
        action = action.strip().capitalize()
        if action not in ["Buy", "Sell"]:
            print(f"Skipping unknown action: {action}")
            continue

        if action == "Buy":
            holdings, cumulative_buy_cost, one, balance_qty, result = process_buy(
                holdings, cumulative_buy_cost, one, balance_qty, date, ticker, qty, price
            )
        else:
            sale_price_val = sale_price[0] if sale_price else price
            holdings, cumulative_buy_cost, one, balance_qty, result = process_sell(
                holdings, cumulative_buy_cost, one, balance_qty, date, ticker, qty, price, sale_price_val
            )

        if result:
            transaction_results.append(result)
            insert_queries.append(tuple(result))

    if insert_queries:
        cursor.executemany("""
            INSERT INTO avgs (
                date, ticker, action, qty, balance_qty, price, sale_price, sell_profit,
                total_cost, sell_total_profit, cumulative_buy_cost, cumulative_total_qty, avg_cost
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, insert_queries)
        db.commit()

    return tabulate(transaction_results, headers=[
        "Date", "Ticker", "Buy/Sell", "Qty", "Balance Qty", "Price", "Sale Price", "Sell Profit",
        "Total Cost", "Sell Total Profit", "Cumulative Total Cost", "Cumulative Total Qty", "Average Cost"
    ], tablefmt="grid")

def store_data_in_db(data):
    try:
        conn = get_connection()
        cursor = conn.cursor()
        result_table = fifo_tracker(data, cursor, conn)
        cursor.close()
        conn.close()
        print("FIFO data stored successfully.")
        return result_table
    except mysql.connector.Error as err:
        error_description = traceback.format_exc()
        log_error_to_db("he_average_cost_scenarios.py", error_description, created_by="fifo_module", env="dev")
        return None
    except Exception as e:
        error_description = traceback.format_exc()
        log_error_to_db("he_average_cost_scenarios.py", error_description, created_by="fifo_module", env="dev")
        return None

def fetch_fifo_data():
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT activity_date, instrument, tran_code, quantity, price
            FROM stock_transactions
            WHERE instrument IS NOT NULL
            ORDER BY instrument ASC;
        """)
        rows = cursor.fetchall()
        cursor.close()
        conn.close()

        print("Fetched Transactions:")
        print(tabulate(rows, headers=["Date", "Ticker", "Action", "Qty", "Price"], tablefmt="grid"))

        return [row + (None,) for row in rows]
    except mysql.connector.Error as err:
        error_description = traceback.format_exc()
        log_error_to_db("he_average_cost_scenarios.py", error_description, created_by="fifo_module", env="dev")
        return []
    except Exception as e:
        error_description = traceback.format_exc()
        log_error_to_db("he_average_cost_scenarios.py", error_description="Database Error: {err}", created_by="fifo_module", env="dev")
        return []

if __name__ == "__main__":
    transactions = fetch_fifo_data()
    if transactions:
        output_table = store_data_in_db(transactions)
        if output_table:
            print("\nFIFO Calculation Result:")
            print(output_table)
    else:
        print("No transactions found to process.")
