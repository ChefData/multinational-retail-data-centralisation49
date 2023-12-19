from classes.data_cleaning import DataCleaning
from classes.data_extraction import DataExtractor
from classes.database_utils import DatabaseConnector


if __name__ == "__main__":
    # Creating Class instances
    data_cleaner = DataCleaning()
    data_extractor = DataExtractor()
    pg_connector = DatabaseConnector('do_not_track/pg_creds.yaml')

    # Raw Products data
    s3_address = 's3://data-handling-public/products.csv'
    raw_products_data = data_extractor.extract_from_s3(s3_address)

    # Printing the original DataFrame
    print("Products Data DataFrame:")
    print(raw_products_data)
    print(raw_products_data.info())

    # Cleaning Products data
    converted_products_data = data_cleaner.convert_product_weights(raw_products_data)
    cleaned_products_data = data_cleaner.clean_products_data(converted_products_data)

    # Printing the cleaned DataFrame
    print("Cleaned Products Data:")
    print(cleaned_products_data)
    print(cleaned_products_data.info())

    # Uploading DataFrame to a specified table
    table_name_to_upload = 'dim_products'
    pg_connector.upload_to_db(cleaned_products_data, table_name_to_upload)
    print(f"Data uploaded to the '{table_name_to_upload}' table in the 'sales_data' PostgreSQL database.")

    # Cast data types - The ? in VARCHAR will be replaced with an integer representing the maximum length of the values in that column.
    column_types = {
        'product_price_£': 'FLOAT',
        'weight_kg'      : 'FLOAT',
        'int_article_no' : 'VARCHAR(?)',
        'product_code'   : 'VARCHAR(?)',
        'weight_class'   : 'VARCHAR(?)',
        'date_added'     : 'DATE',
        'uuid'           : 'UUID',
        'still_available': 'BOOL',
    }
    pg_connector.cast_data_types(table_name_to_upload, column_types)
    print(f"Columns in '{table_name_to_upload}' table have been cast to the following data types: '{column_types}'")

    # Update the respective columns as primary key
    primary_key = 'product_code'

    pg_connector.add_primary_key(table_name_to_upload, primary_key)
    print(f"'{primary_key}' in '{table_name_to_upload}' designated as primary key")
