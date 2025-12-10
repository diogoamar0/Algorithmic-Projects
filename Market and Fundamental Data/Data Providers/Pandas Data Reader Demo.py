import warnings
warnings.filterwarnings('ignore')
import os
from datetime import datetime
import pandas as pd
import pandas_datareader.data as web
import matplotlib.pyplot as plt
import mplfinance as mpf
import seaborn as sns
import requests
import yfinance as yf

#wikipedia list of S&P500 companies
sp_url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"

# Use requests with a valid User-Agent
headers = {"User-Agent": "Mozilla/5.0"}
response = requests.get(sp_url, headers=headers)

# Parse HTML with pandas
sp500_constituents = pd.read_html(response.text, header=0)[0]
#sp500_constituents.head())

#yahoo finance
start = '2014-01-01'
end = '2017-05-24'

# Download data
yahoo = yf.download("META", start=start, end=end)

# Flatten MultiIndex columns
yahoo.columns = yahoo.columns.get_level_values(0)

#print(yahoo.info())

# Plot
#mpf.plot(yahoo, type='candle')
#plt.tight_layout()

#Book Data




