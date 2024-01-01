from classes.data_cleaning import DataCleaning
from classes.data_extraction import DataExtractor
from classes.database_utils import DatabaseConnector
from decouple import config


if __name__ == "__main__":
    # Creating Class instances
    data_cleaner = DataCleaning()
    data_extractor = DataExtractor()
    pg_connector = DatabaseConnector(config('local_db_path'))

    # Raw Card data
    pdf_path = config('pdf_path')
    raw_card_data = data_extractor.retrieve_pdf_data(pdf_path)

    # Printing the original DataFrame
    print("Card Data DataFrame:")
    print(raw_card_data)
    print(raw_card_data.info())

    # Cleaning Card data
    cleaned_card_data = data_cleaner.clean_card_data(raw_card_data)
    
    # Printing the cleaned DataFrame
    print("Cleaned Card Data:")
    print(cleaned_card_data)
    print(cleaned_card_data.info())

    # Uploading DataFrame to a specified table
    table_name_to_upload = 'dim_card_details'
    pg_connector.upload_to_db(cleaned_card_data, table_name_to_upload)
    print(f"Data uploaded to the '{table_name_to_upload}' table in the 'sales_data' PostgreSQL database.")

    # Cast data types - The ? in VARCHAR will be replaced with an integer representing the maximum length of the values in that column.
    column_types = {
        'card_number'           : 'VARCHAR(?)',
        'expiry_date'           : 'VARCHAR(?)',
        'date_payment_confirmed': 'DATE',
    }
    pg_connector.cast_data_types(table_name_to_upload, column_types)
    print(f"Columns in '{table_name_to_upload}' table have been cast to the following data types: '{column_types}'")

    # Update the respective columns as primary key
    primary_key = 'card_number'

    pg_connector.add_primary_key(table_name_to_upload, primary_key)
    print(f"'{primary_key}' in '{table_name_to_upload}' designated as primary key")
