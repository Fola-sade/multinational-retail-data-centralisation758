from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning
import traceback


# Integrating all steps
if __name__ == "__main__":
    try:
        # Initialize objects
        db_connector = DatabaseConnector()
        data_extractor = DataExtractor()
        data_cleaning = DataCleaning()

        # Initialize the local database engine
        local_engine = db_connector.init_db_engine_local()

        # List tables in the database
        tables = db_connector.list_db_tables()
        print(f"Available tables: {tables}")

        # Iterate over all available tables and process each one
        for table in tables:
            # Attempt to extract data from the table
            table_data_df = data_extractor.read_rds_table(db_connector, table)

            if table_data_df is None:
                print(f"No data returned from the '{table}' table. The DataFrame is None.")
                continue  # Skip to the next table

            # Clean the data generically (without specifying column names)
            cleaned_table_data = data_cleaning.clean_user_data(table_data_df)

            if cleaned_table_data is None or cleaned_table_data.empty:
                print(f"No data to upload for the '{table}' table. Cleaned data is None or empty.")
                continue  # Skip to the next table

            # Upload cleaned data to the database, using the same table name
            db_connector.upload_to_db(cleaned_table_data, local_engine, table)

            print(f"Cleaned data uploaded to '{table}' table successfully.")

    except Exception as e:
        # Print a detailed error message with the stack trace for debugging
        print(f"An error occurred: {e}")
        print(traceback.format_exc())
