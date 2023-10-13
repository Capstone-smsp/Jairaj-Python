# data_import.py
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

import sys
import os
sys.path.append('/Users/jairajtilakshettigar/anaconda3/Jairaj Python/')

# Initialize logging
logging.basicConfig(filename='data_import.log', level=logging.DEBUG)

# Database parameters
username = 'capstonesmsp'
password = 'cap@989898'
cluster_name = 'capstone-smsp'
database_name = 'mongodb'
collection_name = 'stock_name_list'

# Database URL
uri = "mongodb+srv://capstonesmsp:KbR3v9zFUlwbIGJX@capstone-smsp.8syk09j.mongodb.net/?retryWrites=true&w=majority"

# Password stored in parameter
encoded_password = urllib.parse.quote(password)

# Create a new client and connect to the server with server API version '1'
client = MongoClient(uri, server_api=ServerApi('1'))

# Function to create a collection if it doesn't exist
def create_collection_if_not_exists(client, database_name, collection_name):
    db = client[database_name]
    if collection_name not in db.list_collection_names():
        db.create_collection(collection_name)
        logging.info(f"Collection '{collection_name}' created successfully")

# Send a ping to confirm a successful connection
try:
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)

# TD Client import API initiate
# API Key: 8a4895518ce540b29fb39f74eb25a641
api_key = "8a4895518ce540b29fb39f74eb25a641"
td = TDClient(apikey="8a4895518ce540b29fb39f74eb25a641")

# Importing stock list from Twelve Data using API in JSON format and getting converted into CSV format

# Define the endpoint to retrieve the list of Canadian stocks
endpoint = 'https://api.twelvedata.com/stocks'

# Parameters
params = {
    'exchange': 'TSX',  # Toronto Stock Exchange
    'country': 'Canada',
    'format': 'JSON',
    'apikey': api_key,
}

# Make the API request
response = requests.get(endpoint, params=params)

# Check if the request was successful
if response.status_code == 200:
    stock_data = response.json()

    # Specify the path to save the CSV file
    csv_file_path = 'canadian_stocks.csv'

    # Open the CSV file for writing
    with open(csv_file_path, mode='w', newline='') as csv_file:
        csv_writer = csv.writer(csv_file)

        # Write the CSV header
        csv_writer.writerow(['Symbol', 'Name', 'Currency'])

        # Extract and write each stock's information to the CSV file
        for stock in stock_data['data']:
            csv_writer.writerow([stock['symbol'], stock['name'], stock['currency']])

    print(f"Data saved to {csv_file_path}")

    # Define the source file and destination file
    source_file = "canadian_stocks.csv"

    # Get the current date and time as a datetime object
    current_datetime = datetime.now()

    # Format the datetime object with date and time
    formatted_datetime = current_datetime.strftime("%Y%m%d_%H%M%S")

    # Define the destination file name with the formatted datetime
    destination_file = f"canadian_stocks_{formatted_datetime}.csv"

    # Copy the file
    shutil.copyfile(source_file, destination_file)

    print("File name updated to:", destination_file)


# Importing data into MongoDB
def stock_list_import(collection_name):
    csv_stock_list_file_name = destination_file
    data_stock_list = pd.read_csv(csv_stock_list_file_name)
    data_dsl_list = data_stock_list.to_dict(orient='records')

    db = client[database_name]
    collection = db[collection_name]

    create_collection_if_not_exists(client, database_name, collection_name)
    
    try:
        collection.insert_many(data_dsl_list)
        print("Data inserted successfully.")
        logging.info(f"Data inserted into '{collection_name}' successfully")
    except Exception as e:
        print(f"Failed to insert data into MongoDB: {str(e)}")
        logging.error(f"Failed to insert data into '{collection_name}': {str(e)}")

def fetch_and_save_historical_data(symbol,database_name, collection_name):


  ##  fetch_and_save_historical_data("AAPL", "your_database_name", "your_collection_name")
    try:
        # Connect to MongoDB
        client = MongoClient(uri, server_api=ServerApi('1'))  # Change the MongoDB URI as needed
        db = client[database_name]

        # Fetch historical data from Yahoo Finance
        stock_data = yf.download(symbol, period="5y")  # Adjust the period as needed

        if stock_data.empty:
            print(f"No data available for {symbol}")
        else:
            # Reset the index to include date as a column
            stock_data.reset_index(inplace=True)

            # Create a collection name based on the symbol
            new_collection_name = f"{symbol}_historical_data"
            collection = db[new_collection_name]

            # Convert the DataFrame to a list of dictionaries
            data_to_insert = stock_data.to_dict(orient='records')

            # Insert the data into the MongoDB collection
            collection.insert_many(data_to_insert)

            print(f"Data imported for {symbol} into {new_collection_name}")
    except Exception as e:
        print(f"Failed to import data for {symbol}: {str(e)}")