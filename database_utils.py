import psycopg2
import pandas as pd
import yaml
from sqlalchemy import create_engine

class DatabaseConnector:
    def __init__(self, db_name, db_user, db_password, db_host = 'localhost', db_port = 5432):
        self.db_name = db_name
        self.db_user = db_user
        self.db_password = db_password
        self.dbhost = db_host
        self.db_port = db_port
        self.connection = None


    def connect(self):
        """
        Establish a connection to the PostgreSQL database.
        """
        try:
            self.connection = psycopg2.connect(
                dbname=self.db_name,
                user=self.db_user,
                password=self.db_password,
                host=self.db_host,
                port=self.db_port
            )
            print("Connection to the database established successfully.")
        except Exception as e:
            print(f"Error connecting to the database: {e}")
    
    def upload_data(self, df, table_name):
        """
        Upload data to the specified database table.
        
        :param df: A pandas DataFrame containing the data to upload
        :param table_name: The name of the table to upload the data to
        """
        if self.connection is None:
            print("No active database connection. Please connect to the database first.")
            return
        
        try:
            cursor = self.connection.cursor()
            for index, row in df.iterrows():
                columns = ', '.join(row.index)
                values = ', '.join([f"'{str(val)}'" for val in row.values])
                insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({values});"
                cursor.execute(insert_query)
            
            self.connection.commit()
            print(f"Data successfully uploaded to {table_name}.")
        
        except Exception as e:
            print(f"Error uploading data: {e}")
        finally:
            cursor.close()

    def close(self):
        """
        Close the database connection.
        """
        if self.connection:
            self.connection.close()
            print("Database connection closed.")


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
        returns : List of table names.

        """

        engine = self.init_db_engine()
        if engine is None:
            print("No database engine available.")
            return None
        
        try:
            with engine.connect() as connection:
                result = connection.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public';")
                tables = [row[0] for row in result]
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

# Integrating all steps
if __name__ == "__main__":
    # Initialize objects
    db_connector = DatabaseConnector()
    data_extractor = DataExtractor()
    data_cleaning = DataCleaning()

    # List tables and extract user data
    tables = db_connector.list_db_tables()
    if 'user_data' in tables:
        user_data_df = data_extractor.read_rds_table(db_connector, 'user_data')

        # Clean the user data
        cleaned_user_data = data_cleaning.clean_user_data(user_data_df)

        # Upload cleaned data to the database
        db_connector.upload_to_db(cleaned_user_data, 'dim_users')
            
