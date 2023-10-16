# Author: Chase Fonte
# Last Updated: 9/30/2023

import yfinance as yf
import pandas as pd
import finsymbols
import datetime
from sqlalchemy import create_engine

# Connect to PostgreSQL Database
engine = create_engine('postgresql://<Username:Password>@localhost:5432/<Database>') # Provide Username:Password@IPAddress:Port/DatabaseName
# Get SPY500 List from Wiki
sp500source = finsymbols.get_sp500_symbols()
sp500 = [s['symbol'].replace('\n','') for s in sp500source] # Clean wiki output for loop
now = datetime.datetime.now()
tickercount = 0
# Establish explicit fields for Database (Used to audit data later)
required = ['symbol', 'last_updated','industry', 'sector', 'fulltimeemployees', 'beta', 'trailingpe', 'forwardpe', 'averagevolume', 'marketcap', 'fiftytwoweeklow', 'fiftytwoweekhigh', 'floatshares', 'sharesoutstanding', 'sharesshort', 'shortratio', 'bookvalue', 'shortname', 'currentprice', 'recommendationkey', 'numberofanalystopinions', 'totalcash', 'ebitda', 'totaldebt', 'totalrevenue', 'debttoequity', 'freecashflow', 'operatingcashflow', 'earningsgrowth', 'revenuegrowth', 'grossmargins', 'ebitdamargins', 'operatingmargins']
# Loop through each Stock Ticker, Normalize(JSON) the data into dataframe friendly columns, insert last obtained equity information
for stock_obj in sp500:
    stock = yf.Ticker(stock_obj)
    info = stock.info
    df = pd.DataFrame.from_dict(pd.json_normalize(info), orient='columns') # Normalize json formatting for Dataframe (Important)
    df.columns = map(str.lower, df.columns)
    df = df[:1]
    df.insert(1, "last_updated", now)
    # Check for missing columns, add with 0 if missing
    for col in required:
        if col not in df.columns:
            df = df.assign(**{col:0})
    # Filter DataFrame to be compatible with Database
    results = df[['symbol', 'last_updated','industry', 'sector', 'fulltimeemployees', 'beta', 'trailingpe', 'forwardpe', 'averagevolume', 'marketcap', 'fiftytwoweeklow', 'fiftytwoweekhigh', 'floatshares', 'sharesoutstanding', 'sharesshort', 'shortratio', 'bookvalue', 'shortname', 'currentprice', 'recommendationkey', 'numberofanalystopinions', 'totalcash', 'ebitda', 'totaldebt', 'totalrevenue', 'debttoequity', 'freecashflow', 'operatingcashflow', 'earningsgrowth', 'revenuegrowth', 'grossmargins', 'ebitdamargins', 'operatingmargins']]
    # Write the DataFrame to the database table
    table_name = 'equity_info'
    results.to_sql(table_name, engine, if_exists='append', index=False)
    tickercount += 1
    print("Written: " + stock_obj)
    print("Submission: " + str(tickercount))

print("\n")
print("Finished: Out of loop")
