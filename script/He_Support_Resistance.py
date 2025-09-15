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
from mplfinance.original_flavor import candlestick_ohlc

from HE_Error_Logs import log_error_to_db


us_eastern = timezone("America/New_York")


root = Tk()
root.title("Stock Chart Viewer (US Eastern Time)")
root.geometry("1100x600")


top_frame = Frame(root)
top_frame.pack(pady=10)

Label(top_frame, text="Enter Stock Symbol: ").pack(side='left')
symbol_entry = Entry(top_frame)
symbol_entry.pack(side='left')

fetch_button = Button(top_frame, text="Search")
fetch_button.pack(side='left', padx=5)

Label(top_frame, text="Select Duration:").pack(side='left', padx=5)
duration_box = Combobox(
    top_frame,
    values=["1 Day", "1 Week", "2 Weeks", "1 Month", "3 Months", "6 Months", "9 Months", "1 Year", "All Year"],
    state="readonly"
)
duration_box.set("1 Year")
duration_box.pack(side='left', padx=5)

Label(top_frame, text="Chart Type:").pack(side='left', padx=5)
chart_type_box = Combobox(top_frame, values=["Line", "Candlestick"], state="readonly")
chart_type_box.set("Line")
chart_type_box.pack(side='left', padx=5)


title_label = Label(root, text="", font=("Arial", 15, "bold"))
title_label.pack(pady=(5, 0))


fig, ax = plt.subplots(figsize=(10, 5))
canvas = FigureCanvasTkAgg(fig, master=root)
canvas_widget = canvas.get_tk_widget()
canvas_widget.pack(expand=True, fill='both')
toolbar = NavigationToolbar2Tk(canvas, root)
toolbar.update()
toolbar.pack(side="bottom", fill="x")


def to_decimal(val, places=2):
    try:
        if isinstance(val, (pd.Series, np.ndarray)):
            val = val.item()
        return float(Decimal(str(val)).quantize(Decimal(f'1.{"0"*places}'), rounding=ROUND_HALF_UP))
    except Exception as e:
        log_error_to_db("he_support_resistance.py", str(e), created_by="to_decimal")
        return 0.0

def localize(df):
    try:
        return df.tz_convert(us_eastern) if df.index.tzinfo else df.tz_localize("UTC").tz_convert(us_eastern)
    except Exception as e:
        log_error_to_db("he_support_resistance.py", str(e), created_by="localize")
        return df


def zoom(event):
    try:
        base_scale = 1.1
        if event.inaxes != ax:
            return
        xlim, ylim = ax.get_xlim(), ax.get_ylim()
        xdata, ydata = event.xdata, event.ydata
        scale_factor = 1 / base_scale if event.button == 'up' else base_scale
        ax.set_xlim([xdata - (xdata - xlim[0]) * scale_factor, xdata + (xlim[1] - xdata) * scale_factor])
        ax.set_ylim([ydata - (ydata - ylim[0]) * scale_factor, ydata + (ylim[1] - ydata) * scale_factor])
        canvas.draw()
    except Exception as e:
        log_error_to_db("he_support_resistance.py", str(e), created_by="zoom")

canvas.mpl_connect("scroll_event", zoom)

prev_xlim, prev_ylim = None, None

def fetch_and_plot(preserve_zoom=True):
    global prev_xlim, prev_ylim
    try:
        symbol = symbol_entry.get().upper().strip()
        if not symbol:
            ax.set_title("Please enter a stock symbol.", fontsize=12)
            canvas.draw()
            return

        if preserve_zoom:
            prev_xlim, prev_ylim = ax.get_xlim(), ax.get_ylim()

        ax.clear()

        duration = duration_box.get()
        chart_type = chart_type_box.get()
        if not duration or not chart_type:
            ax.set_title("Please select duration and chart type.", fontsize=12)
            canvas.draw()
            return

        current_time = datetime.now(us_eastern).strftime('%H:%M:%S')

        duration_map = {
            "1 Day":     ("1d", "5m", 100),
            "1 Week":    ("7d", "1d", 100),
            "2 Weeks":   ("14d", "1d", 150),
            "1 Month":   ("1mo", "1d", 25),
            "3 Months":  ("3mo", "1mo", 100),
            "6 Months":  ("6mo", "1mo", 150),
            "9 Months":  ("9mo", "1mo", 200),
            "1 Year":    ("1y", "1mo", 100),
            "All Year":  ("max", "1mo", 200)
        }

        period, interval, visible_window = duration_map.get(duration, ("1mo", "1d", 30))

        df = yf.download(symbol, period=period, interval=interval, auto_adjust=True)
        if df.empty or 'Close' not in df:
            ax.set_title("No data found", fontsize=12)
            canvas.draw()
            return

        df = localize(df[['Open', 'High', 'Low', 'Close']].dropna())
        df['Date'] = mdates.date2num(df.index.to_pydatetime())
        df_visible = df.iloc[-visible_window:]
        close = df_visible['Close']

        support = to_decimal(close.rolling(min(20, len(df_visible))).min().dropna().iloc[-1])
        resistance = to_decimal(close.rolling(min(20, len(df_visible))).max().dropna().iloc[-1])
        last_price = to_decimal(close.iloc[-1])
        last_time = df_visible.index[-1]
        lower_10 = to_decimal(last_price * 0.90)
        lower_15 = to_decimal(last_price * 0.85)

        candle_width_map = {
            "1m": 0.0005, "5m": 0.001, "1h": 0.005, "1d": 0.2,
            "1wk": 0.5, "1mo": 1.0, "3mo": 1.5, "6mo": 2.0,
            "1y": 2.0, "max": 10.0
        }
        candle_width = candle_width_map.get(interval, 0.5)

        if chart_type == "Line":
            ax.plot(df_visible.index, close, label=f"{symbol} Close", color='skyblue')
        else:
            ohlc = df_visible[['Date', 'Open', 'High', 'Low', 'Close']].values
            candlestick_ohlc(ax, ohlc, width=candle_width, colorup='green', colordown='red')

        ax.axhline(last_price, color='blue', linestyle='--', label=f'Current: ${last_price:.2f}')
        ax.axhline(resistance, color='darkred', linestyle='-.', label=f'Resistance: ${resistance:.2f}')
        ax.axhline(support, color='darkgreen', linestyle='-.', label=f'Support: ${support:.2f}')
        ax.axhline(lower_10, color='orange', linestyle='--', label=f'-10% Drop: ₹{lower_10:.2f}')
        ax.axhline(lower_15, color='purple', linestyle='--', label=f'-15% Drop: ₹{lower_15:.2f}')

        ax.text(last_time, resistance, f'Resistance - ${resistance:.2f}', va='bottom', ha='right', fontsize=9, color='darkred')
        ax.text(last_time, support, f'Support - ${support:.2f}', va='top', ha='right', fontsize=9, color='darkgreen')
        ax.text(last_time, last_price, f'${last_price:.2f}', va='center', ha='left', fontsize=9, color='blue')
        ax.text(last_time, lower_10, f'-10%↓ ₹{lower_10:.2f}', color='orange', va='bottom', ha='right', fontsize=8)
        ax.text(last_time, lower_15, f'-15%↓ ₹{lower_15:.2f}', color='purple', va='bottom', ha='right', fontsize=8)

        formatter_map = {
            "1 Day": mdates.DateFormatter('%b %d\n%H:%M', tz=us_eastern),
            "1 Month": mdates.DateFormatter('%d %b %Y'),
            "3 Months": mdates.DateFormatter('%d %b %Y'),
            "6 Months": mdates.DateFormatter('%b %Y'),
            "9 Months": mdates.DateFormatter('%b %Y'),
            "1 Year": mdates.DateFormatter('%b %Y'),
            "All Year": mdates.DateFormatter('%Y'),
            "default": mdates.DateFormatter('%d %b')
        }

        ax.xaxis.set_major_formatter(formatter_map.get(duration, formatter_map["default"]))
        ax.xaxis.set_major_locator(mdates.AutoDateLocator())
        ax.set_ylabel("Price (USD)")
        plt.xticks(rotation=45)
        ax.grid(True)

        locator_map = {
            "1 Day": mdates.HourLocator(interval=1),
            "1 Month": mdates.DayLocator(interval=2),
            "3 Months": mdates.DayLocator(interval=7),
            "6 Months": mdates.DayLocator(interval=7),
            "9 Months": mdates.MonthLocator(interval=1),
            "1 Year": mdates.MonthLocator(),
            "All Year": mdates.YearLocator(),
            "2 Weeks": mdates.DayLocator()
        }
        ax.xaxis.set_major_locator(locator_map.get(duration, mdates.DayLocator()))

        ax.legend()
        fig.autofmt_xdate()
        fig.tight_layout()

        title_text = f"{symbol} - {duration} US Eastern Time - {current_time} "
        ax.set_title("")
        title_label.config(text=title_text)

        if preserve_zoom and prev_xlim and prev_ylim:
            ax.set_xlim(prev_xlim)
            ax.set_ylim(prev_ylim)
        else:
            ax.set_xlim(df_visible.index[0], df_visible.index[-1])

        canvas.draw()

    except Exception as e:
        log_error_to_db("he_support_resistance.py", str(e), created_by="fetch_and_plot")


def pan_left(event=None):
    try:
        xlim = ax.get_xlim()
        delta = (xlim[1] - xlim[0]) * 0.1
        ax.set_xlim(xlim[0] - delta, xlim[1] - delta)
        canvas.draw()
    except Exception as e:
        log_error_to_db("he_support_resistance.py", str(e), created_by="pan_left")

def pan_right(event=None):
    try:
        xlim = ax.get_xlim()
        delta = (xlim[1] - xlim[0]) * 0.1
        ax.set_xlim(xlim[0] + delta, xlim[1] + delta)
        canvas.draw()
    except Exception as e:
        log_error_to_db("he_support_resistance.py", str(e), created_by="pan_right")

# Bindings
fetch_button.config(command=lambda: fetch_and_plot(preserve_zoom=False))
duration_box.bind("<<ComboboxSelected>>", lambda event: fetch_and_plot(preserve_zoom=False))
chart_type_box.bind("<<ComboboxSelected>>", lambda event: fetch_and_plot(preserve_zoom=False))
root.bind("<Left>", pan_left)
root.bind("<Right>", pan_right)


def live_updater():
    try:
        fetch_and_plot()
    except Exception as e:
        log_error_to_db("he_support_resistance.py", str(e), created_by="live_updater")
    root.after(100, live_updater)


fetch_and_plot(preserve_zoom=False)
live_updater()
root.mainloop()
