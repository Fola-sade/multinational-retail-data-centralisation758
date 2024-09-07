import psycopg2
import pandas as pd

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
