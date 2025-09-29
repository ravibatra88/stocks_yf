import pandas as pd
import os

comb = pd.read_parquet('financials_parquet/combined_financials_trans.parquet ')
print("Combined DataFrame:")
print(comb.head())

print('comb.columns:', comb.columns)
print([x for x in comb.columns if 'ticker' in x])
print(comb.shape)
print(comb['ticker'].nunique())

comb.head(100).to_csv('./temp/comb_head_100.csv')
comb.tail(100).to_csv('./temp/comb_tail_100.csv')

# income = pd.read_parquet('financials_parquet/income_statement.parquet')
# print("Income Statement DataFrame:")
# print(income.head())
# print('income.columns:', income.columns)