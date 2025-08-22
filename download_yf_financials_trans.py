import pandas as pd
import yfinance as yf
import os
import time
import random

# File paths
TICKERS_FILE = 'EQUITY_L.csv'
OUTPUT_DIR = 'financials_parquet'
OUTPUT_FILE_IS = 'income_statement_trans.parquet'
OUTPUT_FILE_BS = 'balance_sheet_trans.parquet'
OUTPUT_FILE_CF = 'cashflow_trans.parquet'
BATCH_SIZE = 10

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
    print("Processing ticker:", i+1)
    try:
        yf_ticker = yf.Ticker(ticker+".NS")
        # wait for for 1 to 5 seconds to avoid rate limits
        time.sleep(random.uniform(1, 5))
        result_is = yf_ticker.financials
        # result_is.to_csv('./temp/result_is_pre_transpose.csv')
        # # transpose the dataframe
        result_is = result_is.T
        # result_is.to_csv('./temp/result_is_post_transpose.csv')
        # add ticker column
        result_is['ticker'] = ticker
        result_is.reset_index(inplace=True)
        # print("result:", result_is)
        new_data_income.append(result_is)
        
        time.sleep(random.uniform(1, 5))
        result_bs = yf_ticker.balance_sheet
        # result_bs.to_csv('./temp/result_bs_pre_transpose.csv')
        result_bs = result_bs.T
        result_bs['ticker'] = ticker
        result_bs.reset_index(inplace=True)
        # result_bs.to_csv('./temp/result_bs_post_transpose.csv', index=False)
        new_data_bs.append(result_bs)
        
        time.sleep(random.uniform(1, 5))
        result_cf = yf_ticker.cashflow
        result_cf = result_cf.T
        result_cf['ticker'] = ticker
        result_cf.reset_index(inplace=True)
        new_data_cashflow.append(result_cf)
        # concatenate income cashflow and balance sheet side by side
        result_cf = result_cf.drop(columns=['index','ticker'])
        result_bs = result_bs.drop(columns=['index','ticker'])
        #result_is = result_is.drop(columns=[])
        combined = pd.concat([result_is, result_bs, result_cf], axis=1)
        new_data_combined.append(combined)
    
        i += 1
        # if i >= 2:
        #     print("Processed 2 tickers, breaking for demonstration purposes.")
        #     break
        
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
    # read existing file , apend new and save again
    existing_data_income = pd.read_parquet(os.path.join(OUTPUT_DIR, OUTPUT_FILE_IS))
    existing_data_bs = pd.read_parquet(os.path.join(OUTPUT_DIR, OUTPUT_FILE_BS))
    existing_data_cashflow = pd.read_parquet(os.path.join(OUTPUT_DIR, OUTPUT_FILE_CF))
    existing_data_combined = pd.read_parquet(os.path.join(OUTPUT_DIR, 'combined_financials_trans.parquet'))
    new_data_income1 = pd.concat([existing_data_income, new_data_income], ignore_index=True)
    new_data_bs1 = pd.concat([existing_data_bs, new_data_bs], ignore_index=True)
    new_data_cashflow1 = pd.concat([existing_data_cashflow, new_data_cashflow], ignore_index=True)
    new_data_combined1 = pd.concat([existing_data_combined, new_data_combined], ignore_index=True)
    
    # set data types for index columns as date and ticker as string
    new_data_income1['index'] = pd.to_datetime(new_data_income1['index'])
    new_data_bs1['index'] = pd.to_datetime(new_data_bs1['index'])
    new_data_cashflow1['index'] = pd.to_datetime(new_data_cashflow1['index'])    
    new_data_combined1['index'] = pd.to_datetime(new_data_combined1['index'])
    new_data_income1['ticker'] = new_data_income1['ticker'].astype(str)
    new_data_bs1['ticker'] = new_data_bs1['ticker'].astype(str)    
    new_data_cashflow1['ticker'] = new_data_cashflow1['ticker'].astype(str)    
    new_data_combined1['ticker'] = new_data_combined1['ticker'].astype(str)

    # foar all other columns set data type as float
    for col in new_data_income1.columns:
        if col not in ['index', 'ticker']:
            new_data_income1[col] = pd.to_numeric(new_data_income1[col], errors='coerce')
    for col in new_data_bs1.columns:
        if col not in ['index', 'ticker']:
            new_data_bs1[col] = pd.to_numeric(new_data_bs1[col], errors='coerce')
    for col in new_data_cashflow1.columns:
        if col not in ['index', 'ticker']:
            new_data_cashflow1[col] = pd.to_numeric(new_data_cashflow1[col], errors='coerce')
    for col in new_data_combined1.columns:
        if col not in ['index', 'ticker']:
            new_data_combined1[col] = pd.to_numeric(new_data_combined1[col], errors='coerce')
    
    # save to csv as caution remove later
    new_data_income1.to_csv(os.path.join(OUTPUT_DIR, 'income_statement_trans.csv'), index=False)
    new_data_bs1.to_csv(os.path.join(OUTPUT_DIR, 'balance_sheet_trans.csv'), index=False)    
    new_data_cashflow1.to_csv(os.path.join(OUTPUT_DIR, 'cashflow_trans.csv'), index=False)    
    new_data_combined1.to_csv(os.path.join(OUTPUT_DIR, 'combined_financials_trans.csv'), index=False)

    new_data_income1.to_parquet(os.path.join(OUTPUT_DIR, OUTPUT_FILE_IS), index=False)
    new_data_bs1.to_parquet(os.path.join(OUTPUT_DIR, OUTPUT_FILE_BS), index=False)
    new_data_cashflow1.to_parquet(os.path.join(OUTPUT_DIR, OUTPUT_FILE_CF), index=False)
    new_data_combined1.to_parquet(os.path.join(OUTPUT_DIR, 'combined_financials_trans.parquet'), index=False)

    
else:
    # set data types for index columns as date and ticker as string
    new_data_income['index'] = pd.to_datetime(new_data_income['index'])
    new_data_bs['index'] = pd.to_datetime(new_data_bs['index'])
    new_data_cashflow['index'] = pd.to_datetime(new_data_cashflow['index'])    
    new_data_combined['index'] = pd.to_datetime(new_data_combined['index'])
    new_data_income['ticker'] = new_data_income['ticker'].astype(str)
    new_data_bs['ticker'] = new_data_bs['ticker'].astype(str)    
    new_data_cashflow['ticker'] = new_data_cashflow['ticker'].astype(str)    
    new_data_combined['ticker'] = new_data_combined['ticker'].astype(str)

    # foar all other columns set data type as float
    for col in new_data_income.columns:
        if col not in ['index', 'ticker']:
            new_data_income[col] = pd.to_numeric(new_data_income[col], errors='coerce')
    for col in new_data_bs.columns:
        if col not in ['index', 'ticker']:
            new_data_bs[col] = pd.to_numeric(new_data_bs[col], errors='coerce')
    for col in new_data_cashflow.columns:
        if col not in ['index', 'ticker']:
            new_data_cashflow[col] = pd.to_numeric(new_data_cashflow[col], errors='coerce')
    for col in new_data_combined.columns:
        if col not in ['index', 'ticker']:
            new_data_combined[col] = pd.to_numeric(new_data_combined[col], errors='coerce')
    
    # save to csv as caution remove later
    new_data_income.to_csv(os.path.join(OUTPUT_DIR, 'income_statement_trans.csv'), index=False)
    new_data_bs.to_csv(os.path.join(OUTPUT_DIR, 'balance_sheet_trans.csv'), index=False)    
    new_data_cashflow.to_csv(os.path.join(OUTPUT_DIR, 'cashflow_trans.csv'), index=False)    
    new_data_combined.to_csv(os.path.join(OUTPUT_DIR, 'combined_financials_trans.csv'), index=False)


    new_data_income.to_parquet(os.path.join(OUTPUT_DIR, OUTPUT_FILE_IS), index=False)
    new_data_bs.to_parquet(os.path.join(OUTPUT_DIR, OUTPUT_FILE_BS), index=False)
    new_data_cashflow.to_parquet(os.path.join(OUTPUT_DIR, OUTPUT_FILE_CF), index=False)
    new_data_combined.to_parquet(os.path.join(OUTPUT_DIR, 'combined_financials_trans.parquet'), index=False)
#new_data.to_parquet(os.path.join(OUTPUT_DIR, OUTPUT_FILE), append=True, ignore_index=True)
print(f"Processed batch of {len(batch)} tickers")
