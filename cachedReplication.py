from google.cloud import storage
import json
import pandas as pd
from sqlalchemy import create_engine
import getpass
import os
import redis
import hashlib


# service_account_key_path = "/home/m23aid064/service-account-file.json"

# Initialize the GCP storage client
def initialize_storage_client(service_account_key_path):
    return storage.Client.from_service_account_json(service_account_key_path)

# Initialize Redis client
redis_client = redis.StrictRedis(host='localhost', port=6379, db=0, decode_responses=True)

def fetch_from_cache(cache_key):
    """Fetch data from Redis cache"""
    cached_data = redis_client.get(cache_key)
    if cached_data:
        return json.loads(cached_data)  # Return data if found in cache
    else:
        return None

def save_to_cache(cache_key, data):
    """Save data to Redis cache"""
    redis_client.set(cache_key, json.dumps(data), ex=3600)  # Expires in 1 hour (3600 seconds)

# Download files from the GCP bucket
def download_nosql_files(bucket_name, prefix, service_account_key_path):
    cache_key = f"nosql_data_{bucket_name}_{prefix}"
    cached_data = fetch_from_cache(cache_key)
    
    if cached_data:
        print("Data fetched from Redis cache")
        return cached_data  # Return cached data if found
    
    # If not cached, download the data as usual
    client = initialize_storage_client(service_account_key_path)
    bucket = client.get_bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix)

    nosql_data = []

    for blob in blobs:
        file_data = blob.download_as_text()
        json_data = json.loads(file_data)
        nosql_data.append(json_data)  # Append each JSON document to a list

    save_to_cache(cache_key, nosql_data)  # Cache the downloaded data
    return nosql_data  # Return the freshly fetched data

# Convert JSON document to DataFrames
def convert_json_to_dataframes(json_data):
    # Create a cache key based on the data
    cache_key = f"dataframe_{hashlib.md5(str(json_data).encode()).hexdigest()}"
    cached_dataframe = fetch_from_cache(cache_key)
    
    if cached_dataframe:
        print("DataFrame fetched from Redis cache")
        return pd.DataFrame(cached_dataframe)
    
    # Normalize JSON data to DataFrame
    df = pd.json_normalize(json_data)
    
    save_to_cache(cache_key, df.to_dict(orient='records'))  # Cache the dataframe as a list of records
    return df  # Return the new dataframe

# Function to write dataframe to SQL
def write_to_sql(dataframe, table_name, engine):
    dataframe.to_sql(table_name, con=engine, if_exists='replace', index=False)

# Automating the entire process
def nosql_to_sql_pipeline(bucket_name, prefix, service_account_key_path, database_uri):
    # Download NoSQL documents from GCP bucket
    nosql_data = download_nosql_files(bucket_name, prefix, service_account_key_path)

    # Convert JSON data to DataFrames
    dataframes = [convert_json_to_dataframes(doc) for doc in nosql_data]

    # Initialize database engine
    engine = create_engine(database_uri)

    # Write each dataframe to a separate SQL table
    for idx, df in enumerate(dataframes):
        table_name = f'table_{idx}'
        write_to_sql(df, table_name, engine)

# Configure SQLAlchemy engine (to be replaced with our database URI)
username = input('Enter username: ')
password = getpass.getpass('Enter password: ')
host = "127.0.0.1"
port = "3306"
database = "SDEProject"
database_uri = f'mysql+pymysql://{username}:{password}@{host}/{database}'
engine = create_engine(database_uri)
service_key = 'service-account-file.json'
bucket_name = 'no_sql_files'
prefix = ''

# Execute the pipeline
nosql_to_sql_pipeline(bucket_name, prefix, service_key, database_uri)