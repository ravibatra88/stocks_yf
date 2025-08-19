import pandas as pd
import yfinance as yf
import os

# File paths
TICKERS_FILE = 'EQUITY_L.csv'
OUTPUT_DIR = 'financials_parquet'
OUTPUT_FILE = 'all.parquet'
BATCH_SIZE = 250

# Ensure output directory exists
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Read tickers
df = pd.read_csv(TICKERS_FILE)
tickers = df['SYMBOL'].dropna().unique().tolist()

# Find already downloaded tickers
downloaded = set()
if os.path.exists(os.path.join(OUTPUT_DIR, OUTPUT_FILE)):
    downloaded = pd.read_parquet(os.path.join(OUTPUT_DIR, OUTPUT_FILE))['ticker'].unique()

# Filter tickers to download
to_download = [t for t in tickers if t not in downloaded]

# Download and append new tickers
batch = to_download[0:BATCH_SIZE]
new_data = []
for ticker in batch:
    try:
        yf_ticker = yf.Ticker(ticker)
        result = yf_ticker.financials
        result['ticker'] = ticker
        result.reset_index(inplace=True)
        new_data.append(result)
    except Exception as e:
        print(f"Error downloading {ticker}: {e}")
new_data = pd.concat(new_data)
new_data.to_parquet(os.path.join(OUTPUT_DIR, OUTPUT_FILE), append=True, ignore_index=True)
print(f"Processed batch of {len(batch)} tickers")
