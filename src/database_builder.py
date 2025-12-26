import twstock  # Import the twstock library to access Taiwan stock market data and info
import yfinance as yf # Import yfinance to fetch stock data from Yahoo Finance
import pandas as pd  # Import pandas for data manipulation and analysis
import numpy as np  # Import numpy for numerical operations
import sqlite3  # Import sqlite3 to interact with SQLite databases
from datetime import datetime  # Import datetime for handling date and time

# Static variable
TWDB_DIR = '../data/twstock.db'
TW_STOCK_CODE = []
TWO_STOCK_CODE = []
COLUMN = ["Date", "Open", "High", "Low", "Close", "Volume", "Type", "Stock_ID"]

# Building database
conn = sqlite3.connect(TWDB_DIR)
cursor = conn.cursor()
cursor.execute("""
CREATE TABLE IF NOT EXISTS tw_stock_prices 
(
        Date DATETIME,
        Stock_ID TEXT,
        Open REAL,
        High REAL,
        Low REAL,
        Close REAL,
        Volume INTEGER,
        Type TEXT,
        PRIMARY KEY (Date, Stock_ID)
);
""")

print("Creating table for TW stock...")
for c in twstock.twse.keys():
    if len(c) == 4:
        print(f"Downloading {c}.TW stock data..., {((len(TW_STOCK_CODE) + len(TWO_STOCK_CODE)) * 100) // 1845} %")
        TW_STOCK_CODE.append(c)
        df = yf.download(f"{c}.TW", period= "5y", auto_adjust=False, multi_level_index= False)
        df = df.reset_index()
        df["Type"] = "TW"
        df["Stock_ID"] = c
        df = df.drop(columns=["Adj Close"], axis= 1)
        df.index.names = ['Date']
        df.to_sql("tw_stock_prices", conn, if_exists="append", index=False)


print("Creating table for TWO stock...")
for c in twstock.tpex.keys():
    if len(c) == 4:
        print(f"Downloading {c}.TWO stock data..., {((len(TW_STOCK_CODE) + len(TWO_STOCK_CODE)) * 100) // 1845} %")
        TWO_STOCK_CODE.append(c)
        df = yf.download(f"{c}.TWO", period="5y", auto_adjust=False, multi_level_index= False)
        df = df.reset_index()
        df["Type"] = "TWO"
        df["Stock_ID"] = c
        df = df.drop(columns=["Adj Close"], axis= 1)
        df.index.names = ['Date']
        df.to_sql("tw_stock_prices", conn, if_exists="append", index=False)





