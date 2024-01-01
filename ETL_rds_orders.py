from classes.data_cleaning import DataCleaning
from classes.data_extraction import DataExtractor
from classes.database_utils import DatabaseConnector
from decouple import config


if __name__ == "__main__":
    # Creating Class instances
    data_cleaner = DataCleaning()
    data_extractor = DataExtractor()
    db_connector = DatabaseConnector(config('rds_db_path'))
    pg_connector = DatabaseConnector(config('local_db_path'))

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
    print(f"Data uploaded to the '{table_name_to_upload}' table in the 'sales_data' PostgreSQL database.")

    # Cast data types - The ? in VARCHAR will be replaced with an integer representing the maximum length of the values in that column.
    column_types = {
        'date_uuid'       : 'UUID',
        'user_uuid'       : 'UUID',
        'card_number'     : 'VARCHAR(?)',
        'store_code'      : 'VARCHAR(?)',
        'product_code'    : 'VARCHAR(?)',
        'product_quantity': 'SMALLINT',
    }
    pg_connector.cast_data_types(table_name_to_upload, column_types)
    print(f"Columns in '{table_name_to_upload}' table have been cast to the following data types: '{column_types}'")

    # Create foreign key constraints that reference the primary keys of the other table.
    foreign_keys = {
        'dim_date_times'   : 'date_uuid',
        'dim_users'        : 'user_uuid',
        'dim_card_details' : 'card_number',
        'dim_store_details': 'store_code',
        'dim_products'     : 'product_code',
    }
    pg_connector.add_foreign_key(table_name_to_upload, foreign_keys)
    print(f"Columns in '{table_name_to_upload}' table have been with the following foreign key constraints: '{foreign_keys}'")
