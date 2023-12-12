from data_cleaning import DataCleaning
from data_extraction import DataExtractor
from database_utils import DatabaseConnector


if __name__ == "__main__":
    # Creating Class instances
    data_cleaner = DataCleaning()
    data_extractor = DataExtractor()
    pg_connector = DatabaseConnector('do_not_track/pg_creds.yaml')

    # Set the API key
    api_key = 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'
    data_extractor.set_api_key(api_key)

    # Endpoints
    number_stores_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
    store_endpoint_template =  'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{}'

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
    
    '''
    print(raw_store_data['continent'].unique())
    print(raw_store_data['country_code'].unique())
    print(raw_store_data['store_type'].unique())
    print(cleaned_store_data['continent'].unique())
    print(cleaned_store_data['country_code'].unique())
    print(cleaned_store_data['store_type'].unique())
    '''
    
    # Uploading DataFrame to a specified table
    table_name_to_upload = 'dim_store_details'
    pg_connector.upload_to_db(cleaned_store_data, table_name_to_upload)
    pg_engine = pg_connector.init_db_engine()
    print(f"Data uploaded to the '{table_name_to_upload}' table in the 'sales_data' PostgreSQL database.\nPostgreSQL Database Engine: '{pg_engine}'")