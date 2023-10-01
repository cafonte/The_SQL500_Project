# Author: Chase Fonte
# Last Updated: 9/30/2023

import yfinance as yf
import pandas as pd
import finsymbols
from sqlalchemy import create_engine

# Connect to PostgreSQL Database
engine = create_engine('postgresql://<Username:Password>@localhost:5432/<Database>') # Provide Username:Password@IPAddress:Port/DatabaseName
# Get SPY500 List from Wiki
sp500source = finsymbols.get_sp500_symbols()
sp500 = [s['symbol'].replace('\n','') for s in sp500source] # Clean wiki output for loop
sp500_list = ["tsla"]
tickercount = 0
# Establish explicit fields for Database (Used to audit data later)
required = ['symbol', 'last_updated', 'free cash flow', 'repayment of debt', 'issuance of debt', 'capital expenditure', 'end cash position', 'beginning cash position', 'changes in cash', 'financing cash flow', 'long term debt payments', 'investing cash flow', 'operating cash flow', 'cash flow from continuing operating activities', 'stock based compensation', 'depreciation']
# Loop through each Stock Ticker, Transpose the data into dataframe friendly columns, insert symbol and last obtained cashflow information
for stock_obj in sp500:
    stock = yf.Ticker(stock_obj)
    stock_inc = stock.quarterly_cashflow
    transposed = stock_inc.transpose()
    transposed.columns = map(str.lower, transposed.columns)
    transposed = transposed[:1]
    df = pd.DataFrame(transposed)
    df.insert(0, "symbol", stock_obj)
    last_update = df.index
    df.insert(1, "last_updated", last_update)
    # Check for missing columns, add with 0 if missing
    for col in required:
        if col not in df.columns:
            df = df.assign(**{col:0})
    # Filter DataFrame to be compatible with Database
    results = df[['symbol', 'last_updated', 'free cash flow', 'repayment of debt', 'issuance of debt', 'capital expenditure', 'end cash position', 'beginning cash position', 'changes in cash', 'financing cash flow', 'long term debt payments', 'investing cash flow', 'operating cash flow', 'cash flow from continuing operating activities', 'stock based compensation', 'depreciation']]
    results.columns = results.columns.str.replace(' ', '_')
    # Write the DataFrame to the database table
    table_name = 'cashflow'
    results.to_sql(table_name, engine, if_exists='append', index=False)
    tickercount += 1
    print("Written: " + stock_obj)
    print("Submission: " + str(tickercount))

print("\n")
print("Finished: Out of loop")
