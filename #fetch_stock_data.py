#fetch_stock_data.py
import yfinance as yf
from sqlalchemy import create_engine, select, Table, MetaData
import pandas as pd
import urllib
import time

SQLALCHEMY_DATABASE_URL = r'sqlite:///C:\\Workspace\\Qriocity\\Other Domain Projects\\Apache Superset_Without_venv\\stock_data.db'
engine = create_engine(SQLALCHEMY_DATABASE_URL)
metadata = MetaData(bind=engine)
stock_data_table = Table('stock_data', metadata, autoload=True, autoload_with=engine)

def check_and_append_data(data, engine):
    with engine.connect() as conn:
        for _, row in data.iterrows():
            try:
                # Extract and format date value
                date_value = pd.to_datetime(row['Date'])
                date_str = date_value.strftime('%Y-%m-%d %H:%M:%S')

                # Format symbol value
                symbol_value = str(row['Symbol']).strip()

                # Create a clean dictionary for the row data
                clean_data = {
                    'Date': date_str,
                    'Symbol': symbol_value,
                    'Dividend': float(row.get('Dividend', 0)),
                    'EPS': float(row.get('EPS', 0)),
                    'PE Ratio': float(row.get('PE Ratio', 0)),
                    'Beta': float(row.get('Beta', 0)),
                    'Adjusted Close': float(row.get('Adj Close', 0)),
                    'Daily Price Range': float(row.get('Daily Price Range', 0)),
                    'Daily Price Change': float(row.get('Daily Price Change', 0)),
                    'Daily Price Change Percentage': float(row.get('Daily Price Change Percentage', 0)),
                    'Total Dollar Volume': float(row.get('Total Dollar Volume', 0)),
                    'Volume Weighted Average Price (VWAP)': float(row.get('Volume Weighted Average Price (VWAP)', 0)),
                    'Price Range Percentage': float(row.get('Price Range Percentage', 0)),
                    'Daily Return': float(row.get('Daily Return', 0)),
                    '50-day SMA': float(row.get('50-day SMA', 0)),
                    '200-day SMA': float(row.get('200-day SMA', 0)),
                    '26-day EMA': float(row.get('26-day EMA', 0)),
                    'MACD Line': float(row.get('MACD Line', 0)),
                    '9-day EMA of MACD': float(row.get('9-day EMA of MACD', 0)),
                    'MACD Histogram': float(row.get('MACD Histogram', 0)),
                    'Volatility': float(row.get('Volatility', 0))
                }

                # Check if record exists
                stmt = select(stock_data_table).where(
                    stock_data_table.c.Date == date_str,
                    stock_data_table.c.Symbol == symbol_value
                )
                
                result = conn.execute(stmt).fetchall()
                
                if not result:
                    # Create DataFrame from clean data
                    row_df = pd.DataFrame([clean_data])
                    
                    # Insert into database
                    row_df.to_sql('stock_data', con=engine, if_exists='append', index=False)
                    print(f"Successfully inserted data for {symbol_value} on {date_str}")
                    
            except Exception as e:
                print(f"Error processing row: {e}")
                print(f"Raw date value: {row['Date']}")
                print(f"Raw symbol value: {row['Symbol']}")



def calculate_RSI(data, periods = 14):
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
    try:
        # Create a copy to avoid modifying the original dataframe
        df = data.copy()
        
        # Ensure column names are strings, not tuples
        df.columns = [col[0] if isinstance(col, tuple) else col for col in df.columns]
        
        # Basic calculations
        df['Daily Price Range'] = df['High'] - df['Low']
        df['Daily Price Change'] = df['Close'] - df['Open']
        df['Daily Price Change Percentage'] = (df['Daily Price Change'] / df['Open']) * 100
        df['Total Dollar Volume'] = df['Volume'] * df['Close']
        df['Volume Weighted Average Price (VWAP)'] = df['Total Dollar Volume'] / df['Volume']
        df['Price Range Percentage'] = ((df['High'] - df['Low']) / df['Open']) * 100
        df['Daily Return'] = df['Close'].pct_change()

        # Fetch financials
        financials = fetch_financials(symbol)
        for key, value in financials.items():
            df[key] = value

        # Add other metrics
        df['Adjusted Close'] = df['Adj Close']
        df['50-day SMA'] = df['Close'].rolling(window=50).mean()
        df['200-day SMA'] = df['Close'].rolling(window=200).mean()
        df['12-day EMA'] = df['Close'].ewm(span=12, adjust=False).mean()
        df['26-day EMA'] = df['Close'].ewm(span=26, adjust=False).mean()
        df['MACD Line'] = df['12-day EMA'] - df['26-day EMA']
        df['9-day EMA of MACD'] = df['MACD Line'].ewm(span=9, adjust=False).mean()
        df['MACD Histogram'] = df['MACD Line'] - df['9-day EMA of MACD']
        df['Volatility'] = df['Daily Return'].rolling(window=30).std()
        df['RSI'] = calculate_RSI(df)

        return df

    except Exception as e:
        print(f"Error in calculating additional metrics for {symbol}: {e}")
        return data


def fetch_and_store_stock_data(symbols):
    for symbol in symbols:
        try:
            # Download data
            data = yf.download(symbol, period="3mo", interval="1d")
            data.reset_index(inplace=True)
            
            # Ensure column names are strings
            data.columns = [col[0] if isinstance(col, tuple) else col for col in data.columns]
            
            # Calculate metrics
            data = calculate_additional_metrics(data, symbol)
            
            # Add symbol column
            data['Symbol'] = symbol
            
            # Store data
            check_and_append_data(data, engine)
            print(f"Data stored for {symbol}")
            
        except Exception as e:
            print(f"Error processing symbol {symbol}: {e}")


# Define list of company symbols
symbols = ["ACN", "AAPL", "CTSH", "IBM", "INFY"]

interval_hours = 1  

while True:
    fetch_and_store_stock_data(symbols)
    print(f"Waiting {interval_hours} hour(s) to fetch data again...")
    time.sleep(interval_hours * 3600)