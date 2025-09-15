import yfinance as yf
import numpy as np
from scipy.stats import norm
from datetime import datetime
import mysql.connector
import traceback

from HE_Database_Connect import get_connection
from HE_Error_Logs import log_error_to_db  # Error logger

# === Black-Scholes Greeks Calculator ===
def black_scholes_greeks(S, K, T, r, sigma, option_type='call'):
    d1 = (np.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * np.sqrt(T))
    d2 = d1 - sigma * np.sqrt(T)

    if option_type == 'call':
        delta = norm.cdf(d1)
        theta = (-S * norm.pdf(d1) * sigma / (2 * np.sqrt(T)) -
                 r * K * np.exp(-r * T) * norm.cdf(d2))
        rho = K * T * np.exp(-r * T) * norm.cdf(d2)
    elif option_type == 'put':
        delta = norm.cdf(d1) - 1
        theta = (-S * norm.pdf(d1) * sigma / (2 * np.sqrt(T)) +
                 r * K * np.exp(-r * T) * norm.cdf(-d2))
        rho = -K * T * np.exp(-r * T) * norm.cdf(-d2)
    else:
        raise ValueError("option_type must be 'call' or 'put'")

    gamma = norm.pdf(d1) / (S * sigma * np.sqrt(T))
    vega = S * norm.pdf(d1) * np.sqrt(T)

    return {
        'Delta': delta,
        'Gamma': gamma,
        'Theta (per day)': theta / 365,
        'Vega (per 1% IV)': vega / 100,
        'Rho (per 1% rate)': rho / 100
    }

# --- USER INPUT ---
user_input = {
    'symbol': 'SPY',
    'expiry_input': '21.5.2025',
    'strike_price': 577.0,
    'option_type': 'call'
}

# --- Parse Input ---
symbol = user_input['symbol'].upper()
expiry = datetime.strptime(user_input['expiry_input'], '%d.%m.%Y').date()
strike_price = user_input['strike_price']
option_type = user_input['option_type'].lower()

# --- Fetch Stock Price ---
try:
    ticker = yf.Ticker(symbol)
    stock_price = ticker.history(period="1d")['Close'].iloc[-1]
except Exception as e:
    error_description = traceback.format_exc()
    log_error_to_db("he_greeks.py", error_description, created_by="greeks_module")
    raise

# --- Time to Expiry ---
today = datetime.today().date()
T = max((expiry - today).days / 365, 1 / 365)

# --- Risk-Free Rate ---
try:
    rfr_data = yf.Ticker("^TNX").history(period="1d")
    risk_free_rate = rfr_data["Close"].iloc[-1] / 100
except Exception as e:
    error_description = traceback.format_exc()
    log_error_to_db("option_greeks.py",  error_description, created_by="greeks_module")
    risk_free_rate = 0.05  # fallback 5%

# --- Get IV from Option Chain ---
expiry_str = expiry.strftime('%Y-%m-%d')
try:
    option_chain = ticker.option_chain(expiry_str)
    option_table = option_chain.calls if option_type == 'call' else option_chain.puts
    option_row = option_table[option_table['strike'] == strike_price]

    if not option_row.empty:
        implied_volatility = option_row['impliedVolatility'].values[0]
        iv_percent = implied_volatility * 100
        print(f"IV for {symbol} {strike_price} {option_type.upper()} (Exp: {expiry_str}) = {iv_percent:.2f}%")
    else:
        print(f"Strike {strike_price} not found in option chain, using fallback IV=20%.")
        implied_volatility = 0.20
except Exception as e:
    error_description = traceback.format_exc()
    log_error_to_db("he_greeks.py", error_description, created_by="greeks_module")
    print(f"Could not fetch option chain: {e}. Using fallback IV=20%.")
    implied_volatility = 0.20

# --- Calculate Greeks ---
try:
    greeks = black_scholes_greeks(
        S=stock_price,
        K=strike_price,
        T=T,
        r=risk_free_rate,
        sigma=implied_volatility,
        option_type=option_type
    )
except Exception as e:
    error_msg = traceback.format_exc()
    log_error_to_db("option_greeks.py", error_msg, created_by="greeks_module")
    raise

# --- Output ---
print(f"\nOption Greeks for {symbol}")
print(f"Spot Price     : ${stock_price:.2f}")
print(f"Strike Price   : {strike_price:.2f}")
print(f"Option Type    : {option_type.upper()}")
print(f"Expiry Date    : {expiry}")
print(f"IV             : {implied_volatility * 100:.2f}%")
print(f"Risk-Free Rate : {risk_free_rate * 100:.2f}%\n")

for greek, value in greeks.items():
    print(f"{greek:<20}: {value:.10f}")

# --- Store in MySQL ---
try:
    conn = get_connection()
    cursor = conn.cursor()

    insert_query = """
    INSERT INTO he_option_greeks (
        symbol, option_type, stock_price, strike_price,
        implied_volatility, risk_free_rate, expiry_date, today_date, time_to_expiry,
        delta, gamma, theta, vega, rho
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    values = (
        symbol,
        option_type,
        stock_price,
        strike_price,
        implied_volatility,
        risk_free_rate,
        expiry.strftime('%Y-%m-%d'),
        today.strftime('%Y-%m-%d'),
        T,
        greeks['Delta'],
        greeks['Gamma'],
        greeks['Theta (per day)'],
        greeks['Vega (per 1% IV)'],
        greeks['Rho (per 1% rate)']
    )

    cursor.execute(insert_query, values)
    conn.commit()
    print("\n Option Greeks stored in the database successfully.")

except mysql.connector.Error as err:
    error_description = traceback.format_exc()
    print(f"\n Database error: {err}")
    log_error_to_db("option_greeks.py", error_description, created_by="greeks_module")

finally:
    if 'cursor' in locals():
        cursor.close()
    if 'conn' in locals():
        conn.close()
