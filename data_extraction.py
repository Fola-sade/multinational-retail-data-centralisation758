import csv
import requests #Extracting data from an api
import boto3  #Extracting data from an AWS S3 Bucket
import pandas as pd
import tabula
from io import StringIO

class DataExtractor:
    def __init__(self):
        pass
        
    def read_rds_table(self, engine, table_name):
        """
        Extracts the specified table from the database and returns it as a pandas DataFrame.
    
        :param engine: A SQLAlchemy engine instance.
        :param table_name: The name of the table to extract.
        :return: pandas DataFrame containing the table data.
        """


        try:
        # Using pandas read_sql_table to read the table into a DataFrame
            df = pd.read_sql_table(table_name, con=engine)
            return df
    
        except Exception as e:
            print(f"Error reading table {table_name}: {e}")
            return None


    def retrieve_pdf_data(self, pdf_link: str) -> pd.DataFrame:
        try:
            # Extract all tables from the PDF file
            df_list = tabula.read_pdf(pdf_link, pages='all', multiple_tables=True)
            
            # Combine all tables into one DataFrame
            combined_df = pd.concat(df_list, ignore_index=True)
            
            return combined_df
        except Exception as e:
            print(f"Error extracting data from PDF: {e}")
            return pd.DataFrame()  # Return empty DataFrame in case of error
    
    def list_number_of_stores(self, endpoint, header):
        '''

        This first get request extracts the store data throough the API; 
        Arg: endpoint and header

        '''
        response = requests.get(url = endpoint, headers = header)
        data = response.json()
        print(data)
        return data['number_stores']
    
    def retrieve_stores_data (self,header, no_of_stores):
        '''This method extracts all the store details using url address and header dictionary
        Args:
            header(dictionary): contains key information
            number_of_stores(int): number of stores whose data need to be extracted
        Returns:
            Dataframe   
            
            '''
        store_data = []

        for i in range(0, no_of_stores):
            endpoint = f"https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{i}"
            response = requests.get(url=endpoint,headers=header)
            data = response.json()
            store_data.append(data)

        df = pd.DataFrame(store_data)
        return df
    

    def extract_from_s3(self,bucket_name, object_key):
        ''' This method extracts data(product data) stored in csv format from S3 bucket on AWS
        It uses boto3 package to download
        Args:
            bucket_name: name of the  S3 bucket on AWS
            object_key: name of the file on bucket
            
        Returns: 
            Dataframe'''
        client = boto3.client('s3')
        obj_csv = client.get_object(Bucket=bucket_name, Key=object_key)
        body = obj_csv['Body']
        csv_string = body.read().decode('utf-8')

        df = pd.read_csv(StringIO(csv_string))

        return df
