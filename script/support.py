import yfinance as yf
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg, NavigationToolbar2Tk
from tkinter import Tk, Frame, Label, Entry, Button
from tkinter.ttk import Combobox
from pytz import timezone
import matplotlib.dates as mdates
from decimal import Decimal, ROUND_HALF_UP
import pandas as pd
import numpy as np
from datetime import datetime

# Timezone
indian_time = timezone("Asia/Kolkata")

# Helper functions
def to_decimal(value, places=2):
    return float(Decimal(value).quantize(Decimal(f'1.{"0"*places}'), rounding=ROUND_HALF_UP))

def safe_scalar(value):
    if isinstance(value, (pd.Series, np.ndarray)):
        return value.item()
    return value

def localize(df):
    if df.index.tzinfo:
        return df.tz_convert(indian_time)
    return df.tz_localize("UTC").tz_convert(indian_time)

# GUI Setup
root = Tk()
root.title("Live NSE/BSE Stock Chart Viewer (IST)")
root.geometry("1200x800")

top_frame = Frame(root)
top_frame.pack(pady=10)

Label(top_frame, text="Enter Stock Symbol (e.g. TCS.NS): ").pack(side='left')
symbol_entry = Entry(top_frame)
symbol_entry.pack(side='left')

fetch_button = Button(top_frame, text="Search")
fetch_button.pack(side='left', padx=5)

Label(top_frame, text="Select Duration:").pack(side='left', padx=4, pady=2)
duration_box = Combobox(top_frame, values=["1 Day", "1 Week", "2 Weeks", "1 Month", "3 Months","6 Months","9 Months", "1 Year", "All Year"], state="readonly")
duration_box.set("1 Day")
duration_box.pack(side='left', padx=5)

fig, ax = plt.subplots(figsize=(10, 4))
canvas = FigureCanvasTkAgg(fig, master=root)
canvas.get_tk_widget().pack(expand=True, fill='both')

# Add toolbar
toolbar = NavigationToolbar2Tk(canvas, root)
toolbar.update()
canvas.get_tk_widget().pack()

# Scroll zoom
def zoom(event):
    base_scale = 1.1
    if event.inaxes != ax:
        return
    xlim = ax.get_xlim()
    ylim = ax.get_ylim()
    xdata, ydata = event.xdata, event.ydata
    if event.button == 'up':
        scale_factor = 1 / base_scale
    elif event.button == 'down':
        scale_factor = base_scale
    else:
        return
    ax.set_xlim([xdata - (xdata - xlim[0]) * scale_factor, xdata + (xlim[1] - xdata) * scale_factor])
    ax.set_ylim([ydata - (ydata - ylim[0]) * scale_factor, ydata + (ylim[1] - ydata) * scale_factor])
    canvas.draw()

canvas.mpl_connect("scroll_event", zoom)

# Core fetch + plot
def fetch_and_plot(event=None):
    ax.clear()
    symbol = symbol_entry.get().upper().strip()
    if not symbol:
        ax.set_title("Please enter a stock symbol.", fontsize=14, fontweight='bold')
        canvas.draw()
        return

    duration = duration_box.get()
    current_ist_time = datetime.now(indian_time).strftime('%H:%M:%S')

    if duration == "1 Day":
        period, interval, visible_window = "1d", "1m", 100
    elif duration == "1 Week":
        period, interval, visible_window = "7d", "1d", 100
    elif duration == "2 Weeks":
        period, interval, visible_window = "14d", "1d", 150
    elif duration == "1 Month":
        period, interval, visible_window = "1mo", "1wk", 25
    elif duration == "3 Months":
        period, interval, visible_window = "3mo", "1mo", 100
    elif duration == "6 Months":
        period, interval, visible_window = "6mo", "1mo", 150
    elif duration == "9 Months":
        period, interval, visible_window = "9mo", "1mo", 200

    elif duration == "All Year":
        period, interval, visible_window = "max", "1mo", 200
    else:
        period, interval, visible_window = "1y", "1mo", 100

    try:
        df = yf.download(symbol, period=period, interval=interval, auto_adjust=True, progress=False)
    except Exception as e:
        ax.set_title(f"Error fetching data: {str(e)}", fontsize=14, fontweight='bold')
        canvas.draw()
        return

    if df.empty:
        ax.set_title(f"No data found for {symbol}", fontsize=14, fontweight='bold')
        canvas.draw()
        return

    df = localize(df)
    close_prices = df["Close"]
    df_visible = df.iloc[-visible_window:]

    ax.plot(df_visible.index, df_visible["Close"], label=f"{symbol} Close", color='skyblue')

    rolling_window = min(20, len(df))
    support_val = close_prices.rolling(rolling_window).min().dropna()
    resistance_val = close_prices.rolling(rolling_window).max().dropna()

    if support_val.empty or resistance_val.empty:
        ax.set_title("Not enough data to calculate support/resistance.", fontsize=14)
        canvas.draw()
        return

    support = to_decimal(safe_scalar(support_val.iloc[-1]))
    resistance = to_decimal(safe_scalar(resistance_val.iloc[-1]))
    last_price = to_decimal(safe_scalar(close_prices.iloc[-1]))
    last_time = df_visible.index[-1]

    lower_10 = to_decimal(last_price * 0.90)
    lower_15 = to_decimal(last_price * 0.85)

    ax.axhline(last_price, color='blue', linestyle='--', label=f'Current: ₹{last_price:.2f}')
    ax.axhline(resistance, color='red', linestyle='-.', label=f'Resistance: ₹{resistance:.2f}')
    ax.axhline(support, color='green', linestyle='-.', label=f'Support: ₹{support:.2f}')
    ax.axhline(lower_10, color='orange', linestyle='--', label=f'10% Drop: ₹{lower_10:.2f}')
    ax.axhline(lower_15, color='purple', linestyle='--', label=f'15% Drop: ₹{lower_15:.2f}')

    # Annotate lines
    ax.text(last_time, resistance, f'₹{resistance:.2f}', color='red', va='bottom', ha='right', fontsize=8)
    ax.text(last_time, support, f'₹{support:.2f}', color='green', va='top', ha='right', fontsize=8)
    ax.text(last_time, last_price, f'₹{last_price:.2f}', color='blue', va='center', ha='left', fontsize=8)
    ax.text(last_time, lower_10, f'10%↓ ₹{lower_10:.2f}', color='orange', va='bottom', ha='right', fontsize=8)
    ax.text(last_time, lower_15, f'15%↓ ₹{lower_15:.2f}', color='purple', va='bottom', ha='right', fontsize=8)

    ax.set_xlim(df_visible.index[0], df_visible.index[-1])
    ax.set_xlabel("Time (IST)")
    ax.set_ylabel("Price (INR)")
    ax.legend()
    ax.grid(True)

    if duration == "1 Day":
        ax.xaxis.set_major_locator(mdates.HourLocator(interval=1))
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M', tz=indian_time))
    elif duration in "1 Year":
        ax.xaxis.set_major_locator(mdates.MonthLocator())
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %Y'))
    elif duration == "All Year":
        ax.xaxis.set_major_locator(mdates.YearLocator())
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
    elif duration == "1 Month":
        ax.xaxis.set_major_locator(mdates.DayLocator(interval=2))
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%d %b %Y'))
    elif duration == "3 Months":
        ax.xaxis.set_major_locator(mdates.DayLocator(interval=7))
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%d %b %Y'))
    elif duration == "6 Months":
        ax.xaxis.set_major_locator(mdates.MonthLocator(interval=1))
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %Y'))
    elif duration == "9 Months":
        ax.xaxis.set_major_locator(mdates.MonthLocator(interval=1))
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %Y'))
    elif duration == "2 Weeks":
        ax.xaxis.set_major_locator(mdates.DayLocator())
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%d %b %Y'))
    else:
        ax.xaxis.set_major_locator(mdates.DayLocator())
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%d %b'))


    ax.set_title(f"{symbol} - {duration} Chart (IST {current_ist_time})", fontsize=14, fontweight='bold')
    fig.autofmt_xdate()
    fig.tight_layout()
    canvas.draw()

# Events
fetch_button.config(command=fetch_and_plot)
duration_box.bind("<<ComboboxSelected>>", fetch_and_plot)

def live_updater():
    fetch_and_plot()
    root.after(1000, live_updater)

fetch_and_plot()
live_updater()

root.mainloop()
