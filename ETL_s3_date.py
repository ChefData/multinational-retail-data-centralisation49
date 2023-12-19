from classes.data_cleaning import DataCleaning
from classes.data_extraction import DataExtractor
from classes.database_utils import DatabaseConnector


if __name__ == "__main__":
    # Creating Class instances
    data_cleaner = DataCleaning()
    data_extractor = DataExtractor()
    pg_connector = DatabaseConnector('do_not_track/pg_creds.yaml')

    # Raw Date data
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

    # Cast data types - The ? in VARCHAR will be replaced with an integer representing the maximum length of the values in that column.
    column_types = {
        'month'      : 'VARCHAR(?)',
        'year'       : 'VARCHAR(?)',
        'day'        : 'VARCHAR(?)',
        'time_period': 'VARCHAR(?)',
        'date_uuid'  : 'UUID',
    }
    pg_connector.cast_data_types(table_name_to_upload, column_types)
    print(f"Columns in '{table_name_to_upload}' table have been cast to the following data types: '{column_types}'")

    # Update the respective columns as primary key
    primary_key = 'date_uuid'

    pg_connector.add_primary_key(table_name_to_upload, primary_key)
    print(f"'{primary_key}' in '{table_name_to_upload}' designated as primary key")
