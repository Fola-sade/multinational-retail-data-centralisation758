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
users_df=data_extractor.read_rds_table(engine,tables[1]) # read table from RDS containing user info
#print(f"Table name is : {tables[1]}")
#print(users_df)
users_df = data_cleaning.clean_user_data(users_df, ['expiry_date', 'date_payment_confirmed']) # clean user data 
#print(users_df)

#Initialize the local database engine
local_engine = db_connector.init_db_engine('db_local_creds.yaml')

#db_connector.upload_to_db(users_df, local_engine,'dim_users')


pdf_df = data_extractor.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')
#print(pdf_df)
pdf_df = data_cleaning.clean_card_data(pdf_df)
#print(pdf_df)
db_connector.upload_to_db(pdf_df, local_engine,'dim_card_details')

