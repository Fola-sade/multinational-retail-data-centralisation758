from sqlalchemy import create_engine
from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning


# Integrating all steps
if __name__ == "__main__":
    # Initialize objects
    db_connector = DatabaseConnector()
    data_extractor = DataExtractor()
    data_cleaning = DataCleaning()

    local_engine = db_connector.init_db_engine_local()

    # List tables and extract user data
    tables = db_connector.list_db_tables()
    if tables is None:
        print("No tables found or error in listing tables.")
    else:
        if 'orders_table' in tables:
            user_data_df = data_extractor.read_rds_table(db_connector, 'orders_table')
 
            # Clean the user data
            cleaned_user_data = data_cleaning.clean_user_data(user_data_df)
          
            # Upload cleaned data to the database
            db_connector.upload_to_db(cleaned_user_data, local_engine, 'dim_users')