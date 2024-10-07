from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning


# Initialize objects
db_connector = DatabaseConnector()    #Object for DatabaseConnector class
data_extractor = DataExtractor()      #Object for DataExtractor Class
data_cleaning = DataCleaning()        #Object for DataCleaning Class 

#Extract, clean and upload user details from yaml file
engine = db_connector.init_db_engine('db_creds.yaml') # get sqlalchemy database engine to estabilish connection to RDS
tables=db_connector.list_db_tables(engine) # get list of tables from RDS
users_df=data_extractor.read_rds_table(engine,tables[2]) # read table from RDS containing user info
#print(users_df)
#print(f"Table name is : {tables[2]}")
#print(users_df)
users_df = data_cleaning.clean_user_data(users_df) # clean user data 
#print(users_df)

#Initialize the local database engine
local_engine = db_connector.init_db_engine('db_local_creds.yaml')

db_connector.upload_to_db(users_df, local_engine,'dim_users')

#PDF
#pdf_df = data_extractor.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')
#print(pdf_df)
#pdf_df = data_cleaning.clean_card_data(pdf_df)
#print(pdf_df)
#db_connector.upload_to_db(pdf_df, local_engine,'dim_card_details')

#API
#Extract, clean and upload store details using API
endpoint = ''
header = {"x-api-key":"" }
#no_of_stores = data_extractor.list_number_of_stores(endpoint, header)
#print(no_of_stores)
#df_stores = data_extractor.retrieve_stores_data (header, no_of_stores)
#df_stores = data_cleaning.clean_store_data(df_stores)
#db_connector.upload_to_db(df_stores, local_engine, 'dim_store_details')

#AWS
#Extract, clean and upload product details boto3 s3 AWS
bucket_name = 'data-handling-public'
object_key = 'products.csv'
#df_product = data_extractor.extract_from_s3(bucket_name,object_key)
#df_product = data_cleaning.convert_product_weights(df_product)
#df_product = data_cleaning. clean_products_data(df_product)
#db_connector.upload_to_db(df_product, local_engine, 'dim_products')

#AWS RDS
#df_order = data_extractor.read_rds_table(engine, 'orders_table')
#df_order = data_cleaning.clean_orders_data(df_order)
#db_connector.upload_to_db(df_order, local_engine, 'orders_table')

#dim_date_times
#Extract and clean and upload dates table
#bucket_name = 'data-handling-public'
#object_key = 'date_details.json'
#df_date = data_extractor. extract_from_s3_date(bucket_name,object_key)
#df_date = data_cleaning.clean_dim_date(df_date)
#db_connector.upload_to_db(df_date, local_engine, 'dim_date_times')
