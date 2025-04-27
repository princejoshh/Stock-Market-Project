#atockapi.py
import sqlite3
import yfinance as yf
import pandas as pd
import time

# Define SQLite connection string
db_file = 'stock_data.db'

def initialize_db():
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS stock_data (
            Date TEXT,
            Symbol TEXT,
            Open REAL,
            High REAL,
            Low REAL,
            Close REAL,
            Adj_Close REAL,
            Volume INTEGER,
            Daily_Price_Range REAL,
            Daily_Price_Change REAL,
            Daily_Price_Change_Percentage REAL,
            Total_Dollar_Volume REAL,
            VWAP REAL,
            Price_Range_Percentage REAL,
            Daily_Return REAL,
            Dividend REAL,
            EPS REAL,
            PE_Ratio REAL,
            Beta REAL,
            Adjusted_Close REAL,
            SMA_50 REAL,
            SMA_200 REAL,
            EMA_12 REAL,
            EMA_26 REAL,
            MACD_Line REAL,
            MACD_Signal REAL,
            MACD_Histogram REAL,
            Volatility REAL,
            RSI REAL,
            PRIMARY KEY (Date, Symbol)
        )
    ''')
    conn.commit()
    conn.close()

def check_and_append_data(data, conn):
    cursor = conn.cursor()
    for index, row in data.iterrows():
        cursor.execute('''
            SELECT * FROM stock_data WHERE Date = ? AND Symbol = ?
        ''', (row['Date'], row['Symbol']))
        result = cursor.fetchone()
        if not result:
            row.to_dict()  # Ensures the row is compatible with SQLite's format
            cursor.execute('''
                INSERT INTO stock_data (
                    Date, Symbol, Open, High, Low, Close, Adj_Close, Volume,
                    Daily_Price_Range, Daily_Price_Change, Daily_Price_Change_Percentage,
                    Total_Dollar_Volume, VWAP, Price_Range_Percentage, Daily_Return,
                    Dividend, EPS, PE_Ratio, Beta, Adjusted_Close, SMA_50, SMA_200,
                    EMA_12, EMA_26, MACD_Line, MACD_Signal, MACD_Histogram, Volatility, RSI
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', tuple(row.values))
    conn.commit()

def calculate_RSI(data, periods=14):
    delta = data['Close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=periods).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=periods).mean()
    RS = gain / loss
    return 100 - (100 / (1 + RS))

def fetch_financials(symbol):
    stock = yf.Ticker(symbol)
    info = stock.info
    return {
        'Dividend': info.get('dividendRate', 0),
        'EPS': info.get('trailingEps', 0),
        'PE Ratio': info.get('trailingPE', 0),
        'Beta': info.get('beta', 0)
    }

def calculate_additional_metrics(data, symbol):
    data['Daily Price Range'] = data['High'] - data['Low']
    data['Daily Price Change'] = data['Close'] - data['Open']
    data['Daily Price Change Percentage'] = (data['Daily Price Change'] / data['Open']) * 100
    data['Total Dollar Volume'] = data['Volume'] * data['Close']
    data['Volume Weighted Average Price (VWAP)'] = data['Total Dollar Volume'] / data['Volume']
    data['Price Range Percentage'] = ((data['High'] - data['Low']) / data['Open']) * 100
    data['Daily Return'] = data['Close'].pct_change()
    financials = fetch_financials(symbol)
    data['Dividend'] = financials['Dividend']
    data['EPS'] = financials['EPS']
    data['PE Ratio'] = financials['PE Ratio']
    data['Beta'] = financials['Beta']
    data['Adjusted Close'] = data['Adj Close']
    data['50-day SMA'] = data['Close'].rolling(window=50).mean()
    data['200-day SMA'] = data['Close'].rolling(window=200).mean()
    data['12-day EMA'] = data['Close'].ewm(span=12, adjust=False).mean()
    data['26-day EMA'] = data['Close'].ewm(span=26, adjust=False).mean()
    data['MACD Line'] = data['12-day EMA'] - data['26-day EMA']
    data['9-day EMA of MACD'] = data['MACD Line'].ewm(span=9, adjust=False).mean()
    data['MACD Histogram'] = data['MACD Line'] - data['9-day EMA of MACD']
    data['Volatility'] = data['Daily Return'].rolling(window=30).std()
    data['RSI'] = calculate_RSI(data)
    return data

def fetch_and_store_stock_data(symbols):
    conn = sqlite3.connect(db_file)
    for symbol in symbols:
        data = yf.download(symbol, period="5mo", interval="1d")
        data.reset_index(inplace=True)
        data = calculate_additional_metrics(data, symbol)
        data['Symbol'] = symbol
        check_and_append_data(data, conn)
        print(f"Data stored for {symbol}")
    conn.close()

# Initialize the database
initialize_db()

# Define list of company symbols
symbols = ["ACN", "AAPL", "CTSH", "IBM", "INFY"]

# Interval to fetch data in hours
interval_hours = 1

while True:
    fetch_and_store_stock_data(symbols)
    print(f"Waiting {interval_hours} hour(s) to fetch data again...")
    time.sleep(interval_hours * 3600)