import functions_framework
from google.cloud import secretmanager
from kaggle.api.kaggle_api_extended import KaggleApi
from google.cloud import storage
import pandas as pd
import numpy as np
import os
import tempfile
from io import StringIO
import psycopg2
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor
from psycopg2.extensions import register_adapter, AsIs
from google.auth.credentials import Credentials
from google.oauth2 import service_account
from google.auth import jwt
import json
import base64

load_dotenv()
register_adapter(np.int64, AsIs)

api = KaggleApi()
api.authenticate()

# credentials = service_account.Credentials.from_service_account_file('alien-grove-405422-bb57fda72219.json')
gcp_key_decoded = base64.b64decode(os.environ.get('gcs_base64'))
credentials_json = json.loads(gcp_key_decoded.decode('utf-8'))
database_pass = base64.b64decode(os.environ.get("DB_PASSWORD")).decode('utf-8')
database_name = base64.b64decode(os.environ.get("DB_NAME")).decode('utf-8')

gcp_credentials = service_account.Credentials.from_service_account_info(credentials_json)
@functions_framework.http
def weekly_stocks(request):

    # Initialize the Secret Manager client
    # client = secretmanager.SecretManagerServiceClient()

    # # Access the secret
    # secret_response = client.access_secret_version(name=secret_name)

    # # Get the secret payload
    # secret_payload = secret_response.payload.data.decode('UTF-8')
    # Get Kaggle credentials from environment variable
    # Set your Kaggle API key
    api = KaggleApi()
    api.authenticate()
    download_dataset(None)
    return "Kaggle API authenticated, Weekly data is extracted, transformed and loaded into Postgres DB"

def connect_db_bulk(df,tbname):
    #Get Credentials
    db_host = os.environ.get("DB_HOST")
    db_port = os.environ.get("DB_PORT")
    db_name = database_name
    db_user = os.environ.get("DB_USER")
    db_password = database_pass

    # Use the variables in your database connection logic
    connection_params = {
        'host': db_host,
        'port': db_port,
        'database': db_name,
        'user': db_user,
        'password': db_password,
    }
    # Establish a connection to your PostgreSQL database
    connection = psycopg2.connect(**connection_params)

    # Create a cursor to execute SQL statements
    cursor = connection.cursor()
    table_name = tbname
    schema_name = 'public'
    df['date'] = pd.to_datetime(df['date']).dt.date
    max_dates_by_company = df.groupby('company')['date'].max()
    # max_dates_by_company_df = max_dates_by_company.reset_index()
    max_dates_by_company = max_dates_by_company.reset_index().sort_values(by='company')
    # print(max_dates_by_company)
    sql_query = "SELECT company, MAX(date) as date FROM public.weekly GROUP BY company ORDER BY company;"
    # Execute the query and store the result in a DataFrame
    db_result = pd.read_sql_query(sql_query, connection)
    db_result['date'] = pd.to_datetime(db_result['date']).dt.date
    res_cd = pd.DataFrame()

    if len(db_result) == 0:
        # res_cd = max_dates_by_company
        insert_query = f"INSERT INTO {schema_name}.{table_name} VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
        records = df.to_records(index=False)
        values = [tuple(record) for record in records]
        cursor.executemany(insert_query, values)
    else:
        db_result['date'] = pd.to_datetime(db_result['date']).dt.date
        for i in range(len(db_result)):
            if db_result.loc[i, "date"] < max_dates_by_company.loc[i, "date"]:
                res_cd = res_cd._append(db_result.loc[i],ignore_index=True)
        
        res = pd.DataFrame()
        for i in range(len(res_cd)):
            company_res = df[(df['company'] == res_cd.loc[i, 'company']) & (df['date'] > res_cd.loc[i, 'date'])]
            res = res._append(company_res,ignore_index=True)
        
        print("result: \n",res)
        if(len(res)!=0):
            insert_query = f"INSERT INTO {schema_name}.{table_name} VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
            records = res.to_records(index=False)
            values = [tuple(record) for record in records]
            cursor.executemany(insert_query, values)

    connection.commit()
    cursor.close()
    connection.close()

def download_dataset(request):
    # Specify the dataset to download
    dataset_name = "nikhil1e9/netflix-stock-price"

    # Set the Cloud Storage bucket name
    bucket_name = "dwdi-de-project"

    # Specify the Cloud Storage directory to store downloaded files
    storage_directory = "testdatasets/"

    # Download the dataset from Kaggle
    with tempfile.TemporaryDirectory() as temp_dir:
        api.dataset_download_files(dataset=dataset_name, path=temp_dir, unzip=True)

        # List downloaded files
        downloaded_files = os.listdir(temp_dir)
        # Upload each file to Cloud Storage
        storage_client = storage.Client(project='My First Project', credentials=gcp_credentials)
        bucket = storage_client.bucket(bucket_name)

        for file_name in downloaded_files:
            local_path = os.path.join(temp_dir, file_name)
            storage_path = os.path.join(storage_directory, file_name)

            # Upload the file to Cloud Storage
            blob = bucket.blob(storage_path)
            blob.upload_from_filename(local_path)

        # Process the uploaded files
        process_uploaded_files(bucket_name, storage_directory)

    return "Dataset downloaded and processed successfully."

def delete_objects(bucket_name, prefix):
    # Create a Storage client
    storage_client = storage.Client()

    # Get the bucket
    bucket = storage_client.bucket(bucket_name)

    # List all objects with the specified prefix
    blobs = bucket.list_blobs(prefix=prefix)

    # Delete each object
    for blob in blobs:
        blob.delete()

def process_uploaded_files(bucket_name, storage_directory):
    data_dict = {"weekly": []}
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    # List files in the Cloud Storage directory
    blobs = list(bucket.list_blobs(prefix=storage_directory))
    weekly_blobs = [blob for blob in blobs if 'weekly' in blob.name]
    print("Weekly blobs: ", weekly_blobs)
    for blob in weekly_blobs:
        # Process only CSV files
        if blob.name.endswith('.csv'):
            # Download the file content
            content = blob.download_as_text()
            # Process the content (example: convert to DataFrame)
            df = pd.read_csv(StringIO(content))
            company=blob.name.split('_')[0]
            company_updated = company.split('/')[1]
            #Append the DataFrame to the 'weekly' list
            data_dict["weekly"].append(df.assign(company=company_updated))

    delete_objects(bucket_name,storage_directory)
    # Concatenate DataFrames
    weekly_data = pd.concat(data_dict["weekly"], ignore_index=True)
    column_mapping = {
    'Date': 'date',
    'Open': 'open',
    'High': 'high',
    'Low': 'low',
    'Close': 'close',
    'Adj Close': 'adj_close',
    'Volume': 'volumn',
    'Company': 'company'
    }
    # Map DataFrame columns to PostgreSQL columns
    df_weekly_mapped = weekly_data.rename(columns=column_mapping)
    connect_db_bulk(df_weekly_mapped,'weekly')
    
    return len(df_weekly_mapped)

