from data_cleaning import DataCleaning
from data_extraction import DataExtractor
from database_utils import DatabaseConnector

if __name__ == "__main__":
    # Creating Class instances
    data_cleaner = DataCleaning()
    data_extractor = DataExtractor()
    pg_connector = DatabaseConnector('do_not_track/pg_creds.yaml')

    s3_address = 's3://data-handling-public/date_details.json'

    raw_date_data = data_extractor.extract_from_s3(s3_address)

    # Printing the original DataFrame
    print("Date DataFrame:")
    print(raw_date_data)
    print(raw_date_data.info())

    # Cleaning Date data
    cleaned_date_data = data_cleaner.clean_date_data(raw_date_data)

    # Printing the cleaned DataFrame
    print("Cleaned Date Data:")
    print(cleaned_date_data)
    print(cleaned_date_data.info())

    # Uploading DataFrame to a specified table
    table_name_to_upload = 'dim_date_times'
    pg_connector.upload_to_db(cleaned_date_data, table_name_to_upload)
    print(f"Data uploaded to the '{table_name_to_upload}' table in the 'sales_data' PostgreSQL database.")