import pandas as pd

class DataCleaning:

    def __init__(self):
        pass
    
    def clean_csv_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean data extracted from a CSV file.
        
        :param df: The pandas DataFrame containing the CSV data.
        :return: A cleaned pandas DataFrame.
        """
        # Example cleaning steps (can be customized)
        # - Drop duplicates
        # - Fill or drop missing values
        # - Remove any irrelevant columns
        
        df_cleaned = df.drop_duplicates()
        df_cleaned = df_cleaned.dropna()  # Handle missing values

        
        print("CSV data cleaned successfully.")
        return df_cleaned

    def clean_api_data(self, data: dict) -> pd.DataFrame:
        """
        Clean data extracted from an API response.
        
        :param data: The raw data from the API response.
        :return: A cleaned pandas DataFrame.
        """
        # Convert the API data (assuming it's in dictionary form) to a DataFrame
        df = pd.DataFrame(data)
        
        # Example cleaning steps (can be customized)
        # - Normalize the structure
        # - Remove unnecessary keys or columns
        # - Convert data types as needed
        
        df_cleaned = df.dropna()  # Handle missing values
        
        print("API data cleaned successfully.")
        return df_cleaned

    def clean_s3_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean data extracted from an S3 bucket.
        
        :param df: The pandas DataFrame containing the S3 data.
        :return: A cleaned pandas DataFrame.
        """
        # Example cleaning steps (can be customized)
        # - Handle missing or null values
        # - Remove unwanted columns
        # - Standardize formats (dates, strings, etc.)
        
        df_cleaned = df.fillna('Unknown')  # Example of handling missing values
        
        print("S3 data cleaned successfully.")
        return df_cleaned
    
    def clean_user_data(self, df):
        """
        Cleans the user data by handling NULL values, fixing date issues, and filtering out incorrect data.
        
        :param df: DataFrame containing user data.
        :return: Cleaned DataFrame.
        """
        # Example cleaning steps:
        df.dropna(subset=['user_id'], inplace=True)  # Remove rows where user_id is null
        df['registration_date'] = pd.to_datetime(df['registration_date'], errors='coerce')  # Convert dates
        df = df[df['age'] > 0]  # Remove rows with invalid age
        
        return df



import csv
import requests #Extracting data from an api
import boto3  #Extracting data from an AWS S3 Bucket
import pandas as pd

class DataExtractor:
    def __init__(self):
        pass
    #Method to extract from a CSV doc
    def extract_from_csv(self, filepath):
        try:
            data = pd.read_csv(filepath)
            print(f"Successfully extracted data from {filepath}")
            return data
        except Exception as e:
            print(f"Error extracting data from csv: {e}")
            return None
    #Method to extract data from an API
    def extract_from_api(self, api_url, headers = None):
        try:
            response = requests.get(api_url, headers=headers)
            response.raise_for_status()
            data = response.json()
            print(f"Successfully extracted data from API: {api_url}")
            return data
        except Exception as e:
            print(f"HTTP error: {e}")
        except Exception as e:
            print(f"Error extracting data from API: {e}")
            return None
        
    def extract_from_s3(self, bucket_name, file_key, aws_access_key, aws_secret_key):
        try:
            s3 = boto3.client(
                's3', 
                aws_access_key_id = aws_access_key,
                aws_secret_access_key = aws_secret_key
            )
            obj = s3.get_object(Bucket = bucket_name, Key = file_key)
            data = obj['Body'].read().decode('utf-8')
            print(f"Successfully extracted data from S3 bucket: {bucket_name}")
            return data
        except Exception as e:
            print(f"Error extracting data from S3: {e}")
            return None
        
    def read_rds_table(self, db_connector, table_name):
        """
        Extracts the specified table from the database and returns it as a pandas DataFrame.
        
        :param db_connector: An instance of the DatabaseConnector class.
        :param table_name: The name of the table to extract.
        :return: pandas DataFrame containing the table data.
        """
        engine = db_connector.init_db_engine()
        if engine is None:
            print("No database engine available.")
            return None
        
        try:
            df = pd.read_sql_table(table_name, con=engine)
            return df
        
        except Exception as e:
            print(f"Error reading table {table_name}: {e}")
            return None     
        print(df)   

import psycopg2
import pandas as pd
import yaml
from sqlalchemy import create_engine
from sqlalchemy import text  # Import text function

class DatabaseConnector:
    def __init__(self):
        pass

    def read_db_creds(self, filepath = 'db_creds.yaml'):
        try:
            with open(filepath, 'r') as file:
                creds = yaml.safe_load(file)
            return creds
        except Exception as e:
            print(f"Error reading the credentials file: {e}")
            return None 

    def init_db_engine(self):
        """
        Initializes and returns a SQLAlchemy engine based on the provided dtabase credentials.

        """
        creds = self.read_db_creds()
        if creds is None:
            return None
        try:
            engine = create_engine(f"postgresql://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}")
            return engine
        except Exception as e:
            print(f"Error initializing database engine: {e}")
            return None
        
    def list_db_tables(self):
        """
        Lists all tables in the connected database.
        Returns: List of table names or None if an error occurs.
        """
        engine = self.init_db_engine()
        if engine is None:
            print("No database engine available.")
            return None
    
        try:
            with engine.connect() as connection:
                result = connection.execute(text("SELECT table_name FROM information_schema.tables WHERE table_schema='public';"))
                tables = [row[0] for row in result]
                if not tables:
                   print("No tables found in the database.")
                   return None
                print(f"Tables in the database: {tables}")
                return tables
        except Exception as e:
            print(f"Error listing tables: {e}")
            return None

    def upload_to_db(self, df, table_name):
        """
        Uploads a pandas DataFrame to the specified table in the database.
        
        :param df: DataFrame to upload.
        :param table_name: The name of the table to upload the data to.
        """
        engine = self.init_db_engine()
        if engine is None:
            print("No database engine available.")
            return

        try:
            df.to_sql(table_name, con=engine, if_exists='replace', index=False)
            print(f"Data uploaded to {table_name} successfully.")
        except Exception as e:
            print(f"Error uploading data: {e}")    


            