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
            df['date_payment_confirmed'] = pd.to_datetime(df['date_payment_confirmed'], errors='coerce')

            # create a regex pattern to match rows with random letters and numbers
            pattern = r'^[a-zA-Z0-9]*$'

            # create a boolean mask for missing or random values
            mask = (df['date_payment_confirmed'].isna()) | (df['date_payment_confirmed'].astype(str).str.contains(pattern))

            # drop the rows with missing or random values
            df = df[~mask]

            # Define a regular expression that matches all non-numeric characters
            pattern = r'[^0-9]'

            # Use the replace() method to remove all non-numeric characters from the column
            df.loc[:, 'card_number'] = df['card_number'].replace(pattern, '', regex=True)

            # print("Longest length is:\n", df.expiry_date.str.len().max())
            print("Longest length is:", df['card_number'].astype(str).str.len().max())

            return df
        except Exception as e:
            print(f"Error cleaning card data: {e}")
            return None  
        
    def clean_store_data(self, df):
        '''This method cleans data(store data) retrieved through API  and returns dataframe'''
        df.replace(r'(<NA>|N/A|NULL)', pd.NA, inplace=True,regex=True)
        df['address'] = df['address'].replace(r'\n', ',', regex=True)
        print(df)
        df.replace(r'^\s*$', pd.NA, regex=True,inplace=True) # replacing blank whitespaces
        df.replace(r'[A-Z0-9]{10}', pd.NA, inplace=True,regex=True)
        #taking out the locality from the address as we already have a column which stores locality
        res=df['address'].str.rpartition(',')
        df['address'] = res[0]
        #drop the column lat as all values in this column are none
        df.drop('lat', axis=1, inplace=True)
        # clean incorrect values in continent column
        df['continent'] = df['continent'].apply(lambda x: x[2:] if pd.notna(x) and x[:2] == 'ee' else x)
        
        #print("Columns in DataFrame:", df.columns.tolist())
        #formatting dates
        df['opening_date'] = pd.to_datetime(df['opening_date'], errors='coerce')
        #print("Columns in DataFrame:", df.columns.tolist())
        #print(type(df))
        
        # clean text from staff_numbers column
        df['staff_numbers'] = df['staff_numbers'].str.replace('[^0-9]', '', regex=True) 
        #find duplicates
        duplicated_store = df.duplicated(subset=['address', 'opening_date','locality'],keep=False)
        #print(store_df[duplicated_store])
        #drop rows where all the columns in subset are null 
        df.dropna(how='all',subset=['address','store_code','locality'],inplace=True)
        #convert the datatype of staff_numbers column to int
        df['staff_numbers'] = np.floor(pd.to_numeric(df['staff_numbers'], errors='coerce')).astype('Int64')
        # to efficiently use the memory convert following columns to category type
        df['continent']= df['continent'].astype('category')
        df['country_code']= df['country_code'].astype('category')
        return df    
    

    def convert_product_weights(self, df):
        '''This method is used to clean the weight column from product data and returns dataframe'''
       
        df['weight'] = df['weight'].fillna('missing')
        df['weight'].replace(r'\d+ x \d+', 'missing', inplace= True, regex=True) #reglar expression to replace 'x' symbols 

        df['weight'].replace(r'[A-Z0-9]{10}', 'missing', inplace= True, regex=True)
        df['weight'].replace(r'77\s\w*', '', inplace= True, regex=True)

        mappings_weight = {'missingg': 'missing'}
        df['weight'].replace(mappings_weight, inplace= True)

        def conversion_in_kg(weight):
            '''Method to strip unit strings, convert to float data type, and convert to kilograms.'''
            
            if weight[-2:] == 'kg':
                return float(weight[:-2])
            elif weight.find(' x ') != -1:
                return eval(weight.replace(' x ', '*')[:-1]) / 1000
            elif weight[-1] == 'g' or weight[-2:] == 'ml' or weight.find('.') != -1:
                return float(re.sub('[^0-9]', '', weight)) / 1000
            elif weight[-2:] == 'oz':
                return float(weight[:-2]) * 0.0283495
            else:
                return weight
                        

        df['weight'] = df['weight'].apply(conversion_in_kg)
        df['weight'].replace('missing', np.nan, inplace=True)
    
        df['weight'] = pd.to_numeric(df['weight'], errors='coerce') # this will replace the values that cannot be converted to numeric with NaN
        df['weight'] = df['weight'].astype('float64')

        return df
    

    def clean_products_data(self, df):
        '''This method cleans product data and returns clean dataframe'''
        
        df['product_price'] = df['product_price'].str.replace('£','')
        df=self.replace_invalid_strings_to_null(df,'product_price')
        duplicate_products = df.duplicated(subset=['product_name', 'weight','category','product_code'],keep=False)
        df.dropna(how= 'all', subset=['product_name', 'category','product_code'], inplace=True)
        df=self.replace_invalid_strings_to_null(df,'category')
        df['category']= df['category'].astype('category')
        df=self.replace_invalid_strings_to_null(df,'removed')
        df=self.clean_date(df,'date_added')
        df.drop(df[df['user_uuid'].str.len() != 36].index, inplace=True)
        return df