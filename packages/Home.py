# Home.py
print("Stock Market Analysis")
print("Project started")

import sys
import os
import requests
import pymongo
import urllib.parse
import csv
import requests
import pandas as pd
import shutil
import yfinance as yf
from bs4 import BeautifulSoup
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from datetime import datetime
import logging
from twelvedata import TDClient

sys.path.append('/Users/jairajtilakshettigar/anaconda3/Jairaj Python/')


from packages import data_import

# Database parameters
username = 'capstonesmsp'
password = 'cap@989898'
cluster_name = 'capstone-smsp'
database_name = 'mongodb'
collection_name = 'stock_name_list'

# Initialize MongoDB client and access the database and collection
client = data_import.client
db = client[data_import.database_name]

def get_tsx_ticker_symbols():
    try:
        urly = 'https://ca.finance.yahoo.com/lookup?s=%5ETSX'
        response = requests.get(urly)

        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            table = soup.find('table')
            ticker_symbol = [row.find_all('td')[0].text.strip() for row in table.find_all('tr')[1:]]
            for tsl in ticker_symbol:
                print(tsl)
            return ticker_symbol

        else:
            print("block get_tsx_ticker_symbols failed")
            return []
    
    except Exception as e:
        print(f"Error: {e}")
        return []



# Ensure that the create_collection_if_not_exists function is called
data_import.create_collection_if_not_exists(client, data_import.database_name, collection_name)

# Import data into MongoDB
data_import.stock_list_import(collection_name)

# Retrieve TSX ticker symbols
tsx_imported_symbols = get_tsx_ticker_symbols()

for symbol in tsx_imported_symbols:
    print("Displaying imported symbol data")
    print(symbol)
    data_import.fetch_and_save_historical_data(symbol,database_name,collection_name)