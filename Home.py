import os
import pymongo
import urllib.parse
import csv
import requests
import pandas as pd
import shutil


from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from twelvedata import TDClient
from datetime import datetime

print ("Stock Market Analysis");
print ("Project started")


##Databae parameters

username = 'capstonesmsp'
password = 'cap@989898'
cluster_name = 'capstone-smsp'
database_name = 'mongodb'

##Database url
uri = "mongodb+srv://capstonesmsp:KbR3v9zFUlwbIGJX@capstone-smsp.8syk09j.mongodb.net/?retryWrites=true&w=majority"

##password stored in parameter
encoded_password = urllib.parse.quote(password)

# Create a new client and connect to the server with server API version '1'
client = MongoClient(uri, server_api=ServerApi('1'))

# Send a ping to confirm a successful connection
try:
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)

##TD Client import API initiate
## API Key: 8a4895518ce540b29fb39f74eb25a641
api_key="8a4895518ce540b29fb39f74eb25a641"
td = TDClient(apikey="8a4895518ce540b29fb39f74eb25a641")

##importing stock list from twelevlve data using api in JSON format and getting converted into csv format

# Define the endpoint to retrieve the list of Canadian stocks
endpoint = 'https://api.twelvedata.com/stocks'

#parameters 
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
    with open(csv_file_path, 'w', newline='') as csv_file:
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

print ("File name updated to:",destination_file)


##Importing data into MongoDB

csv_stock_list_file_name=destination_file
collection_name = "stock_name_list"
data_stock_list = pd.read_csv(csv_stock_list_file_name)
data_dsl_list = data_stock_list.to_dict(orient='records')

db = client[database_name]
collection = db[collection_name]
collection.insert_many(data_dsl_list)
cursor = collection.find()
