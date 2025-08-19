import yfinance as yf
import pandas as pd

# Define the ticker symbol
ticker_symbol = "ACL.NS"

# Create a Ticker object
ticker = yf.Ticker(ticker_symbol)

# Fetch financials data
financials_data = ticker.financials

# Convert to dataframe
financials_df = pd.DataFrame(financials_data)

# Print the dataframe
print("Financials:")
print(financials_df)
