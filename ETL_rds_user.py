from classes.data_cleaning import DataCleaning
from classes.data_extraction import DataExtractor
from classes.database_utils import DatabaseConnector


if __name__ == "__main__":
    # Creating Class instances
    data_cleaner = DataCleaning()
    data_extractor = DataExtractor()
    db_connector = DatabaseConnector('do_not_track/db_creds.yaml')
    pg_connector = DatabaseConnector('do_not_track/pg_creds.yaml')

    # Listing and printing all tables in the database
    db_tables = db_connector.list_db_tables()
    print("Database Tables:", db_tables)
    
    # Using list_db_tables to get the name of the table containing user data
    user_data_table = db_connector.list_db_tables()[1]
    
    # Using read_rds_table to extract the table containing user data
    raw_user_data = data_extractor.read_rds_table(db_connector, user_data_table)
    
    # Printing the original DataFrame
    print("User Data DataFrame:")
    print(raw_user_data)
    print(raw_user_data.info())

    # Cleaning user data
    cleaned_user_data = data_cleaner.clean_user_data(raw_user_data)
    
    # Printing the cleaned DataFrame
    print("Cleaned User Data:")
    print(cleaned_user_data)
    print(cleaned_user_data.info())

    # Uploading DataFrame to a specified table
    table_name_to_upload = 'dim_users'
    pg_connector.upload_to_db(cleaned_user_data, table_name_to_upload)
    print(f"Data uploaded to the '{table_name_to_upload}' table in the 'sales_data' PostgreSQL database.")

    # Cast data types - The ? in VARCHAR will be replaced with an integer representing the maximum length of the values in that column.
    column_types = {
        'first_name'   : 'VARCHAR(255)',
        'last_name'    : 'VARCHAR(255)',
        'country_code' : 'VARCHAR(?)',
        'user_uuid'    : 'UUID' ,
        'join_date'    : 'DATE',
        'date_of_birth': 'DATE',
    }
    pg_connector.cast_data_types(table_name_to_upload, column_types)
    print(f"Columns in '{table_name_to_upload}' table have been cast to the following data types: '{column_types}'")

    # Update the respective columns as primary key
    primary_key = 'user_uuid'

    pg_connector.add_primary_key(table_name_to_upload, primary_key)
    print(f"'{primary_key}' in '{table_name_to_upload}' designated as primary key")
