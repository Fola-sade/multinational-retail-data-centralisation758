from datetime import datetime
import pandas as pd
import numpy as np 
import re

class DataCleaning:

    def __init__(self):
        pass
    
    
    def clean_user_data(self, df):
        """
        Cleans a DataFrame by handling NULL values, converting dates, and filtering out invalid data.
        The cleaning process will adapt based on the columns present in the DataFrame.

        :param df: DataFrame to clean.
        :return: Cleaned DataFrame, or None if the input is invalid.
        """
        # clean invalid dates
        df['join_date'] = pd.to_datetime(df['join_date'], errors='coerce')
        df['date_of_birth'] = pd.to_datetime(df['date_of_birth'], errors='coerce')
        # clean_country_code column
        df['country_code'] = df['country_code'].astype('string')
        mapping = {'GGB':'GB'}
        df['country_code'] = df['country_code'].replace(r'[A-Z0-9]{10}', pd.NA, regex=True)
        df['country_code'] = df['country_code'].replace(mapping)
        df['country_code'] = df['country_code'].replace('', pd.NA) 
        df = df.replace('NULL', pd.NA)
        df.dropna(axis='index',how = 'all', subset=['first_name','last_name'], inplace=True)
        # cleaning phone number
        # remove country codes and/or extensions from phone numbers
        df['phone_number'] = df['phone_number'].str.replace('\+1|\+44|\+49|x\w+', '', regex=True)
        # remove non-numeric characters from phone numbers
        df['phone_number'] = df['phone_number'].str.replace('\D+', '', regex=True)

        # drop rows where unique user id is not standard 36 characters in length
        df.drop(df[df['user_uuid'].str.len() != 36].index, inplace=True)

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
        df.replace(r'^\s*$', pd.NA, regex=True) # replacing blank whitespaces
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
        df.dropna(how='all',subset=['address','store_code','locality'], inplace=True)
        #convert the datatype of staff_numbers column to int
        df['staff_numbers'] = np.floor(pd.to_numeric(df['staff_numbers'], errors='coerce')).astype('Int64')
        # to efficiently use the memory convert following columns to category type
        df['continent']= df['continent'].astype('category')
        df['country_code']= df['country_code'].astype('category')
        return df    
    

    def convert_product_weights(self, df):
        '''This method is used to clean the weight column from product data and returns dataframe'''
       
        df['weight'] = df['weight'].fillna('missing')
        df['weight'] = df['weight'].replace(r'\d+ x \d+', 'missing', regex=True) #reglar expression to replace 'x' symbols 

        df['weight'] = df['weight'].replace(r'[A-Z0-9]{10}', 'missing', regex=True)
        df['weight'] = df['weight'].replace(r'77\s\w*', '', regex=True)

        mappings_weight = {'missingg': 'missing'}
        df['weight'] = df['weight'].replace(mappings_weight)

        def conversion_in_kg(weight):
            '''Method to strip unit strings, convert to float data type, and convert to kilograms.'''
            if weight == 'missing' or weight == '':  # Handle missing or empty weights
               return np.nan
    
            weight = weight.lower().strip()  # Normalize the weight string for easier matching

            # Convert based on units
            if weight.endswith('kg'):
               return float(weight[:-2])  # Remove 'kg' and convert to float
            elif weight.endswith('g'):
               return float(weight[:-1]) / 1000  # Convert grams to kilograms
            elif weight.endswith('oz'):
               return float(weight[:-2]) * 0.0283495  # Convert ounces to kilograms
            elif weight.endswith('ml'):
               return float(weight[:-2]) / 1000  # Assume density of 1g/ml for conversion
            else:
                # Try to strip non-numeric characters and convert remaining numbers
                numeric_weight = re.sub('[^0-9.]', '', weight)
                # Ensure only one decimal point is present
                if numeric_weight.count('.') <= 1 and numeric_weight:  
                    return float(numeric_weight) / 1000 if numeric_weight else np.nan
                else:
                    return np.nan  # Return NaN for invalid cases
                        

        df['weight'] = df['weight'].apply(conversion_in_kg)
        df['weight'] = df['weight'].replace('missing', np.nan)
    
        df['weight'] = pd.to_numeric(df['weight'], errors='coerce') # this will replace the values that cannot be converted to numeric with NaN
        df['weight'] = df['weight'].astype('float64')

        return df
    

    def clean_products_data(self, df):
        '''This method cleans product data and returns clean dataframe'''
        
        df['product_price'] = df['product_price'].str.replace('£','')
        df['product_price'] = df['product_price'].replace(r'[A-Z0-9]{10}', pd.NA, regex=True)
        duplicate_products = df.duplicated(subset=['product_name', 'weight','category','product_code'],keep=False)
        df.dropna(how= 'all', subset=['product_name', 'category','product_code'], inplace=True)
        df['category'] = df['category'].replace(r'[A-Z0-9]{10}', pd.NA, regex=True)
        df['category']= df['category'].astype('category')
        df['removed'] = df['removed'].replace(r'[A-Z0-9]{10}', pd.NA, regex=True)
        df['date_added'] = pd.to_datetime(df['date_added'], errors='coerce')
        #print(df.columns)
        df.drop(df[df['uuid'].str.len() != 36].index, inplace=True)
        return df
    
    def clean_orders_data(self, df):
        '''
        This method will remove the columns first_name, last_name,and 1
        '''
        df = df.drop(columns =['first_name', 'last_name', '1'])
        
        return df
    
    def clean_dim_date(self, df):
        #formatting timestamp  and repalcing with null for inconsistent values
        df['timestamp']= pd.to_datetime(df['timestamp'], format="%H:%M:%S", errors='coerce') # this adds default date to timestamp
        df['timestamp']= df['timestamp'].dt.time # extracting only timestamp values
        #formatting month, year and day columns
        df = self.date_parts_clean(df,'month')
        df = self.date_parts_clean(df,'year')
        df = self.date_parts_clean(df,'day')
        df['time_period'] = df['time_period'].replace(r'[A-Z0-9]{10}', pd.NA, regex=True)
        df = df.replace('NULL', pd.NA)
        df = df.drop(df[df['date_uuid'].str.len() != 36].index)
        df.dropna(how = 'all', subset=['timestamp', 'month', 'year', 'day', 'time_period','date_uuid'], inplace=True)
        return df
    
    def date_parts_clean(self,df,column):
        df[column] = np.round(pd.to_numeric(df[column], errors='coerce')) #this function replaces non-numeric values with null
        df[column]= df[column].fillna(0) # filling null values with 0
        df[column]= df[column].astype(int) # converting month column to int to takeout decimal points
        df[column] = df[column].replace(0,pd.NA)
        return df