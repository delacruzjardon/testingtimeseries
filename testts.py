from pymongo import MongoClient
import time
import pymongo
import random
from datetime import datetime, timezone, timedelta
from bson.objectid import ObjectId

# Connect to MongoDB
client = MongoClient("mongodb+srv://admin:xxxx@xxx.xxxx.mongodb.net/?retryWrites=true&w=majority&appName=xxxxx")
db = client['performanceDB']
client.drop_database('performanceDB') 


# Create collections
normal_collection = db['normalCollection']
timeseries_collection = db.create_collection(
    'timeseriesCollection',
    timeseries={
        'timeField': 'timestamp',
        'metaField': 'metadata',
        'granularity': 'minutes'
    }
)

normal_collection.create_index([("metadata.stockName", pymongo.ASCENDING), ("metadata.currency", pymongo.ASCENDING)])

def generate_random_date_minutes(start_year=2000, end_year=2023, tzinfo=None):
    # Generate random year, month, day, hour, and minute within the given range
    random_year = random.randint(start_year, end_year)
    random_month = random.randint(1, 12)
    # Handle different numbers of days in months
    if random_month in [1, 3, 5, 7, 8, 10, 12]:
        max_day = 31
    elif random_month in [4, 6, 9, 11]:
        max_day = 30
    else:
        # February
        if (random_year % 4 == 0 and random_year % 100 != 0) or (random_year % 400 == 0):
            max_day = 29  # Leap year
        else:
            max_day = 28

    random_day = random.randint(1, max_day)
    random_hour = random.randint(0, 23)
    random_minute = random.randint(0, 59)

    # Create datetime object
    random_date = datetime(random_year, random_month, random_day, random_hour, random_minute, tzinfo=tzinfo)
    
    return random_date



def generate_random_date(start_date, end_date):
    """Generates a random date between the specified start and end dates."""
    delta = end_date - start_date
    random_days = random.randint(0, delta.days)
    return start_date + timedelta(days=random_days)

def generate_documents(document_count):
    """Generates a list of documents with random dates and stock data."""
    documents = []
    start_date = datetime(2022, 1, 1)
    end_date = datetime(2025, 1, 31)
    stock_names = ['Apple', 'Orange', 'Banana']
    currencies = ['Dollar', 'Euro']
    for i in range(document_count):
        document = {
            "_id": ObjectId(),
            "timestamp": generate_random_date_minutes(2020, 2024),
            "metadata": {
                "stockName": random.choice(stock_names),
                "currency": random.choice(currencies)
            },
            "stockPrice": random.uniform(50.0, 150.0)
        }
        documents.append(document)
    return documents

# Function to insert data
def insert_sample_data(collection, is_timeseries=False):
    data = generate_documents(500000)
    start_time = time.time()
    collection.insert_many(data)
    end_time = time.time()
    operation = "Time-series" if is_timeseries else "Normal"
    #print(f"{operation} Collection Insert Time: {end_time - start_time} seconds")



# Query data
def query_data(collection, is_timeseries=False):
    start_time = time.time()
    pipeline = [
        {"$group": {"_id":{ "stockName": "$metadata.stockName", "currency": "$metadata.currency" }, "avgstockPrice": {"$avg": "$stockPrice"}}}
    ]
    result = list(collection.aggregate(pipeline))
     #print(result)
    end_time = time.time()
    operation = "Time-series" if is_timeseries else "Normal"
    print(f"{operation} Collection Query Time: {end_time - start_time} seconds")



# Perform insertions
insert_sample_data(normal_collection)
insert_sample_data(timeseries_collection, is_timeseries=True)
# Perform queries
query_data(normal_collection)
query_data(timeseries_collection, is_timeseries=True)
