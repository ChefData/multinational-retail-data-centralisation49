from classes.data_cleaning import DataCleaning
from classes.data_extraction import DataExtractor
from classes.database_utils import DatabaseConnector


if __name__ == "__main__":
    # Creating Class instances
    data_cleaner = DataCleaning()
    data_extractor = DataExtractor()
    pg_connector = DatabaseConnector('do_not_track/pg_creds.yaml')

    pdf_path = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
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