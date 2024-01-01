from classes.data_cleaning import DataCleaning
from classes.data_extraction import DataExtractor
from classes.database_utils import DatabaseConnector
from decouple import config


if __name__ == "__main__":
    # Creating Class instances
    data_cleaner = DataCleaning()
    data_extractor = DataExtractor()
    pg_connector = DatabaseConnector(config('local_db_path'))

    # Set the API key
    api_key = config('api_key')
    data_extractor.set_api_key(api_key)

    # Endpoints
    number_stores_endpoint = config('number_stores_endpoint')
    store_endpoint_template = config('store_endpoint_template')

    # Get the number of stores
    number_stores = data_extractor.list_number_of_stores(number_stores_endpoint)
    print(f"Number of stores: {number_stores}")

    # Retrieve store data
    raw_store_data = data_extractor.retrieve_stores_data(store_endpoint_template, number_stores)

    # Printing the original DataFrame
    print("Store Data DataFrame:")
    print(raw_store_data)
    print(raw_store_data.info())

    # Cleaning Store data
    cleaned_store_data = data_cleaner.called_clean_store_data(raw_store_data)
    
    # Printing the cleaned DataFrame
    print("Cleaned Store Data:")
    print(cleaned_store_data)
    print(cleaned_store_data.info())
    
    # Uploading DataFrame to a specified table
    table_name_to_upload = 'dim_store_details'
    pg_connector.upload_to_db(cleaned_store_data, table_name_to_upload)
    print(f"Data uploaded to the '{table_name_to_upload}' table in the 'sales_data' PostgreSQL database.")

    # Cast data types - The ? in VARCHAR will be replaced with an integer representing the maximum length of the values in that column.
    column_types = {
        'store_type'   : 'VARCHAR(255)',
        'locality'     : 'VARCHAR(255)',
        'continent'    : 'VARCHAR(255)',
        'country_code' : 'VARCHAR(?)',
        'store_code'   : 'VARCHAR(?)',
        'staff_numbers': 'SMALLINT',
        'opening_date' : 'DATE',
        'longitude'    : 'FLOAT',
        'latitude'     : 'FLOAT',
    }
    pg_connector.cast_data_types(table_name_to_upload, column_types)
    print(f"Columns in '{table_name_to_upload}' table have been cast to the following data types: '{column_types}'")

    # Update the respective columns as primary key
    primary_key = 'store_code'

    pg_connector.add_primary_key(table_name_to_upload, primary_key)
    print(f"'{primary_key}' in '{table_name_to_upload}' designated as primary key")