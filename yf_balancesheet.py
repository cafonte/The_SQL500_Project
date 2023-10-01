# Author: Chase Fonte
# Last Updated: 9/30/2023

import yfinance as yf
import pandas as pd
import finsymbols
from sqlalchemy import create_engine

# Connect to PostgreSQL Database
engine = create_engine('postgresql://<Username:Password>@localhost:5432/<Database>') # Provide Username:Password@IPaddress:Port/DatabaseName
# Get SPY500 List from Wiki
sp500source = finsymbols.get_sp500_symbols()
sp500 = [s['symbol'].replace('\n','') for s in sp500source] # Clean wiki output for loop
tickercount = 0
# Establish explicit fields for Database (Used to audit data later)
required = ['symbol', 'last_updated', 'ordinary shares number', 'share issued', 'total debt', 'tangible book value', 'invested capital',
                'working capital', 'common stock equity', 'total capitalization', 'stockholders equity',
                'retained earnings', 'long term debt', 'current liabilities', 'current deferred liabilities',
                'current deferred revenue', 'current debt', 'total assets', 'inventory', 'cash and cash equivalents']
# Loop through each Stock Ticker, Transpose the data into dataframe friendly columns, insert symbol and last obtained Balance Sheet
for stock_obj in sp500:
    stock = yf.Ticker(stock_obj)
    stock_bal = stock.quarterly_balance_sheet
    transposed = stock_bal.transpose()
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
    results = df[['symbol', 'last_updated', 'ordinary shares number', 'share issued', 'total debt', 'tangible book value', 'invested capital',
                'working capital', 'common stock equity', 'total capitalization', 'stockholders equity',
                'retained earnings', 'long term debt', 'current liabilities', 'current deferred liabilities',
                'current deferred revenue', 'current debt', 'total assets', 'inventory', 'cash and cash equivalents']]
    results.columns = results.columns.str.replace(' ', '_')
    # Write the DataFrame to the database table
    table_name = 'balance_sheet'
    results.to_sql(table_name, engine, if_exists='append', index=False)
    tickercount += 1
    print("Written: "+stock_obj)
    print("Submission: "+str(tickercount))

print("\n")
print("Finished: Out of loop")

