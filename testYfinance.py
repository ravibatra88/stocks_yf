import yfinance as yf
import pandas as pd

# Define the ticker symbol
ticker_symbol = "ACL.NS"

# Create a Ticker object
ticker = yf.Ticker(ticker_symbol)

# Fetch financials data
financials_data = ticker.financials
balance_sheet_data = ticker.balance_sheet
cashflow_data = ticker.cashflow

# Convert to dataframe
financials_df = pd.DataFrame(financials_data)
bs_df = pd.DataFrame(balance_sheet_data)
cashflow_df = pd.DataFrame(cashflow_data)

# Print the dataframe
print("Financials:")
print(financials_df)

print("\nBalance Sheet:")
print(bs_df)
print("\nCash Flow:")
print(cashflow_df)
