# Author: Chase Fonte
# Last Updated: 10/13/2023

import yfinance as yf
import pandas as pd
import finsymbols
from sqlalchemy import create_engine

# Connect to PostgreSQL Database
engine = create_engine('postgresql://<Username:Password>@localhost:5432/<Database>') # Provide Username:Password@IPAddress:Port/DatabaseName
print("<--  Lets Begin | Database Connection Established  -->")
# Get SPY500 List from Wiki
sp500source = finsymbols.get_sp500_symbols()
sp500 = [s['symbol'].replace('\n','') for s in sp500source] # Clean wiki output for loop
print("S&P500 List Acquired..")
# Execute a SQL query and load the results into a DataFrame
db_df = pd.read_sql_query('SELECT symbol, max(last_updated) FROM "cashflow" GROUP BY symbol', con=engine)
print("DB Cashflow Acquired.."+"\n")
tickercount = 0
num_skipped = 0
num_empty = 0
# Establish explicit fields for Database (Used to audit data later)
required = ['symbol', 'last_updated', 'free cash flow', 'repayment of debt', 'issuance of debt', 'capital expenditure', 'end cash position', 'beginning cash position', 'changes in cash', 'financing cash flow', 'long term debt payments', 'investing cash flow', 'operating cash flow', 'cash flow from continuing operating activities', 'stock based compensation', 'depreciation']
# Loop through each Stock Ticker, Transpose the data into dataframe friendly columns, insert symbol and last obtained cashflow information
for stock_obj in sp500:
    db_lookup = db_df[db_df['symbol'] == stock_obj]
    # [0 = first column(symbol), 1 = second column(last_updated)
    mdate = '' if db_lookup.empty else db_lookup.iat[0, 1]
    stock = yf.Ticker(stock_obj)
    stock_inc = stock.quarterly_cashflow
    transposed = stock_inc.transpose()
    transposed.columns = map(str.lower, transposed.columns)
    transposed = transposed[:1]
    df = pd.DataFrame(transposed)
    df.insert(0, "symbol", stock_obj)
    last_update = df.index
    df.insert(1, "last_updated", last_update)
    if mdate == '':
        print("<-- He's dead Jim! -->\nEmpty Dataframe: " + stock_obj+"\n")
        num_empty += 1
    elif last_update <= mdate:
        print("Mom: We already have that at home.\nAt Home: " + stock_obj+"\n")
        num_skipped += 1
    else:
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
        print("Submission: " + str(tickercount)+"\n")

engine.dispose()
print("\n"+"<--  Finished | Database Connection Disposed | Results Below  -->"+"\n")

process_report = pd.DataFrame({
    'Submissions': [tickercount],
    'Skip/No Updates': [num_skipped],
    'Missing Data': [num_empty]
})
print(process_report)
print("\n")
