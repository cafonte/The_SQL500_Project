# The SQL500 Project

Personal project that holds the Balance, Income, Cashflow and Current Equity Info of every company in the S&P 500. Sourced data from Yahoo Finance (yfinance) into a PostgreSQL Database.

Ongoing models/financial strength tests can then be performed against all companies at once where data is available.

![SQL500 Diagram-7](https://github.com/cafonte/The_SQL500_Project/assets/109887258/5f7379be-1fb5-4423-87e8-d4e2b2badaeb)

<img width="728" alt="SQL500 Visualized" src="https://github.com/cafonte/yfinance_to_database/assets/109887258/06c92528-d763-4d92-87fb-26e9a84294fe">

Current Issues:
- yfinance 'info' is broken and isn't retrieving data
- yfinance is inconsistent and has missing data
<img width="1179" alt="Screenshot 2023-10-29 at 8 21 02 AM" src="https://github.com/cafonte/The_SQL500_Project/assets/109887258/0f097f3c-ea16-4f59-a38a-7ac3ce92a660">

- max(last_updated) needs fixed

New Features in Progress: 
- X/Tweets
-   Tested Tweet from Python: https://x.com/ChaseFonte/status/1718281459327270986?s=20
- Switch Balance Sheet/Income/Cashflow Source data from yfinance to SEC EDGAR
- Potentially switch current market data source away from yfinance

