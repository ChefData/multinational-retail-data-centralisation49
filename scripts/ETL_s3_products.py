from classes.data_cleaning import DataCleaning
from classes.data_extraction import DataExtractor
from classes.database_utils import DatabaseConnector


if __name__ == "__main__":
    # Creating Class instances
    data_cleaner = DataCleaning()
    data_extractor = DataExtractor()
    pg_connector = DatabaseConnector('do_not_track/pg_creds.yaml')

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