import os
import pymongo
import urllib.parse
import csv
import requests
import pandas as pd
import shutil
import yfinance as yf
import sys
sys.path.append('/Users/jairajtilakshettigar/anaconda3/Jairaj Python/')

from bs4 import BeautifulSoup
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from twelvedata import TDClient
from datetime import datetime

username = 'capstonesmsp'
password = 'cap@989898'
cluster_name = 'capstone-smsp'
database_name = 'mongodb'
collection_name = "stock_name_list"

##Database url
uri = "mongodb+srv://capstonesmsp:KbR3v9zFUlwbIGJX@capstone-smsp.8syk09j.mongodb.net/?retryWrites=true&w=majority"

##password stored in parameter
encoded_password = urllib.parse.quote(password)

# Create a new client and connect to the server with server API version '1'
client = MongoClient(uri, server_api=ServerApi('1'))


from packages import data_import

data_import.db_connect()

def data_fetch_all_data():
    db =client[database_name]
    collection = db[collection_name]
    dsa=collection.find({},{'Symbol':1})
    
    for x in dsa:
        print(x['Symbol'])