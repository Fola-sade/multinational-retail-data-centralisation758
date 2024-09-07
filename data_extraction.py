import csv
import requests #Extracting data from an api
import boto3  #Extracting data from an AWS S3 Bucket
import pandas as pandas

class DataExtractor:
    def __init__(self):
        pass
    #Method to extract from a CSV doc
    def extract_from_csv(self, filepath):
        try:
            data_csv = pd.read_csv(filepath)
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
        
        


