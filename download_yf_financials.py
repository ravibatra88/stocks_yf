import pandas as pd
import yfinance as yf
import os
import time
import random

# File paths
TICKERS_FILE = 'EQUITY_L.csv'
OUTPUT_DIR = 'financials_parquet'
OUTPUT_FILE_IS = 'income_statement.parquet'
OUTPUT_FILE_BS = 'balance_sheet.parquet'
OUTPUT_FILE_CF = 'cashflow.parquet'
BATCH_SIZE = 200

# Ensure output directory exists
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Read tickers
df = pd.read_csv(TICKERS_FILE)
tickers = df['SYMBOL'].dropna().unique().tolist()

# Find already downloaded tickers
downloaded = set()
if os.path.exists(os.path.join(OUTPUT_DIR, OUTPUT_FILE_IS)):
    downloaded = pd.read_parquet(os.path.join(OUTPUT_DIR, OUTPUT_FILE_IS))['ticker'].unique()

# Filter tickers to download
to_download = [t for t in tickers if t not in downloaded]

# Download and append new tickers
batch = to_download[0:BATCH_SIZE]
new_data_income = []
new_data_bs = []
new_data_cashflow = []
new_data_combined = []
i = 0
for ticker in batch:
    try:
        yf_ticker = yf.Ticker(ticker+".NS")
        # wait for for 1 to 5 seconds to avoid rate limits
        time.sleep(random.uniform(1, 5))
        result_is = yf_ticker.financials
        # result_is.to_csv('./temp/result_is_pre_transpose.csv')
        # # transpose the dataframe
        # result_is = result_is.T
        # result_is.to_csv('./temp/result_is_post_transpose.csv')
        # add ticker column
        result_is['ticker'] = ticker
        result_is.reset_index(inplace=True)
        # print("result:", result_is)
        new_data_income.append(result_is)
        
        time.sleep(random.uniform(1, 5))
        result_bs = yf_ticker.balance_sheet
        # result_bs.to_csv('./temp/result_bs_pre_transpose.csv')
        # result_bs = result_bs.T
        result_bs['ticker'] = ticker
        result_bs.reset_index(inplace=True)
        # result_bs.to_csv('./temp/result_bs_post_transpose.csv', index=False)
        new_data_bs.append(result_bs)
        
        time.sleep(random.uniform(1, 5))
        result_cf = yf_ticker.cashflow
        # result_cf = result_cf.T
        result_cf['ticker'] = ticker
        result_cf.reset_index(inplace=True)
        new_data_cashflow.append(result_cf)
        # concatenate income cashflow and balance sheet side by side
        # result_cf = result_cf.drop(columns=['index','ticker'])
        # result_bs = result_bs.drop(columns=['index','ticker'])
        # result_is = result_is.drop(columns=['index'])
        combined = pd.concat([result_is, result_bs, result_cf])
        new_data_combined.append(combined)
    
        i += 1
        if i >= 2:
            print("Processed 2 tickers, breaking for demonstration purposes.")
            break
        
    except Exception as e:
        print(f"Error downloading {ticker}: {e}")
new_data_income = pd.concat(new_data_income, ignore_index=True )
print("new_data income:", new_data_income)

new_data_bs = pd.concat(new_data_bs, ignore_index=True)
print("new_data balance sheet:", new_data_bs)

new_data_cashflow = pd.concat(new_data_cashflow, ignore_index=True) 
print("new_data cashflow:", new_data_cashflow)

new_data_combined = pd.concat(new_data_combined, ignore_index=True)
print("new_data combined:", new_data_combined)
new_data_income.columns = new_data_income.columns.map(str)
new_data_bs.columns = new_data_bs.columns.map(str)
new_data_cashflow.columns = new_data_cashflow.columns.map(str)
new_data_combined.columns = new_data_combined.columns.map(str)

# Save to parquet files
if os.path.exists(os.path.join(OUTPUT_DIR, OUTPUT_FILE_IS)):
    new_data_income.to_parquet(os.path.join(OUTPUT_DIR, OUTPUT_FILE_IS), index=False,append=True)
    new_data_bs.to_parquet(os.path.join(OUTPUT_DIR, OUTPUT_FILE_BS), index=False,append=True)
    new_data_cashflow.to_parquet(os.path.join(OUTPUT_DIR, OUTPUT_FILE_CF), index=False,append=True)
    new_data_combined.to_parquet(os.path.join(OUTPUT_DIR, 'combined_financials.parquet'), index=False,append=True)
else:
    new_data_income.to_parquet(os.path.join(OUTPUT_DIR, OUTPUT_FILE_IS), index=False)
    new_data_bs.to_parquet(os.path.join(OUTPUT_DIR, OUTPUT_FILE_BS), index=False)
    new_data_cashflow.to_parquet(os.path.join(OUTPUT_DIR, OUTPUT_FILE_CF), index=False)
    new_data_combined.to_parquet(os.path.join(OUTPUT_DIR, 'combined_financials.parquet'), index=False)
#new_data.to_parquet(os.path.join(OUTPUT_DIR, OUTPUT_FILE), append=True, ignore_index=True)
print(f"Processed batch of {len(batch)} tickers")
