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

    def read_db_local_creds(self, filepath = 'db_local_creds.yaml'):
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

    def init_db_engine_local(self):
        """
        Initializes and returns a SQLAlchemy engine based on the provided dtabase credentials.

        """
        creds = self.read_db_local_creds()
        if creds is None:
            return None
        try:
            engine = create_engine(f"{'postgresql'}+{'psycopg2'}://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}")
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

    def upload_to_db(self, df, engine, table_name):
        """
        Uploads a pandas DataFrame to the specified table in the database.
        
        :param df: DataFrame to upload.
        :param table_name: The name of the table to upload the data to.
        """

        try:
            df.to_sql(table_name, con=engine, if_exists='replace', index=False)
            print(f"Data uploaded to {table_name} successfully.")
        except Exception as e:
            print(f"Error uploading data: {e}")    


#f"postgresql://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}"