from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning

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