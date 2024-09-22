import pandas as pd
import numpy as np 
from datetime import datetime
import re

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
    
    
    def clean_user_data(self, df, date_columns = None):
        """
        Cleans a DataFrame by handling NULL values, converting dates, and filtering out invalid data.
        The cleaning process will adapt based on the columns present in the DataFrame.

        :param df: DataFrame to clean.
        :return: Cleaned DataFrame, or None if the input is invalid.
        """
        # Ensure that df is a valid DataFrame before proceeding
        if df is None:
            print("Error: Received None as the DataFrame input.")
            return None

        if df.empty:
            print("Error: Received an empty DataFrame.")
            return None

        try:
            # Drop rows with any missing values (can be adjusted based on the use case)
            df.dropna(inplace=True)

            # Convert any columns that are recognized as dates
            if date_columns:
                for col in date_columns:
                    if col in df.columns:
                        # Try converting columns that might be dates
                        try:
                            df[col] = pd.to_datetime(df[col], format='%Y-%m-%d %H:%M:%S', errors='coerce')
                        except Exception as e:
                            print(f"Error converting column {col} to datetime: {e}")
                
            # Remove any rows where numerical values are invalid (e.g., age less than 0)
            numerical_columns = df.select_dtypes(include=['number']).columns
            for col in numerical_columns:
                if col == 'age':
                    df = df[df[col] > 0]  # Remove rows where 'age' is <= 0

        except Exception as e:
            print(f"An error occurred while cleaning the data: {e}")
            return None

        return df
    
    def clean_card_data(self, df):
        """
        Cleans the card data by removing NULL values and fixing formatting errors.

        :param df: DataFrame containing card data.
        :return: Cleaned DataFrame.
        """
        if df is None or df.empty:
            print("Error: Received None or empty DataFrame.")
            return None

        try:
            # Remove rows with all null values
            df.dropna(how='all', inplace=True)
            df['date_payment_confirmed'] = pd.to_datetime(df['date_payment_confirmed'], infer_datetime_format=True, errors='coerce')

            # create a regex pattern to match rows with random letters and numbers
            pattern = r'^[a-zA-Z0-9]*$'

            # create a boolean mask for missing or random values
            mask = (df['date_payment_confirmed'].isna()) | (df['date_payment_confirmed'].astype(str).str.contains(pattern))

            # drop the rows with missing or random values
            df = df[~mask]

            # Define a regular expression that matches all non-numeric characters
            pattern = r'[^0-9]'

            # Use the replace() method to remove all non-numeric characters from the column
            df['card_number'] = df['card_number'].replace(pattern, '', regex=True)

            # print("Longest length is:\n", df.expiry_date.str.len().max())
            print("Longest length is:", df['card_number'].astype(str).str.len().max())

            return df
        except Exception as e:
            print(f"Error cleaning card data: {e}")
            return None  