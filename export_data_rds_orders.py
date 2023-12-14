from data_cleaning import DataCleaning
from data_extraction import DataExtractor
from database_utils import DatabaseConnector

if __name__ == "__main__":
    # Creating Class instances
    data_cleaner = DataCleaning()
    data_extractor = DataExtractor()
    db_connector = DatabaseConnector('do_not_track/db_creds.yaml')
    pg_connector = DatabaseConnector('do_not_track/pg_creds.yaml')

    # Reading and printing database credentials
    db_creds = db_connector.read_db_creds()
    print("Database Credentials:", db_creds)

    # Initializing and printing the database engine
    db_engine = db_connector.init_db_engine()
    print("Database Engine:", db_engine)

    # Listing and printing all tables in the database
    db_tables = db_connector.list_db_tables()
    print("Database Tables:", db_tables)
    
    # Using list_db_tables to get the name of the table containing user data
    orders_data_table = db_connector.list_db_tables()[2]
    
    # Using read_rds_table to extract the table containing user data
    raw_orders_data = data_extractor.read_rds_table(db_connector, orders_data_table)

    # Printing the original DataFrame
    print("Store Orders DataFrame:")
    print(raw_orders_data)
    print(raw_orders_data.info())

    # Cleaning Store data
    cleaned_orders_data = data_cleaner.clean_orders_data(raw_orders_data)
    
    # Printing the cleaned DataFrame
    print("Cleaned Orders Data:")
    print(cleaned_orders_data)
    print(cleaned_orders_data.info())
    
    # Uploading DataFrame to a specified table
    table_name_to_upload = 'orders_table'
    pg_connector.upload_to_db(cleaned_orders_data, table_name_to_upload)
    pg_engine = pg_connector.init_db_engine()
    print(f"Data uploaded to the '{table_name_to_upload}' table in the 'sales_data' PostgreSQL database.\nPostgreSQL Database Engine: '{pg_engine}'")