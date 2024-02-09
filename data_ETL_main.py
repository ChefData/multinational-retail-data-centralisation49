from classes.data_cleaning import DataCleaning
from classes.data_extraction import DataExtractor
from classes.database_utils import DatabaseConnector
from classes.data_upload import DataLoader
from decouple import config


if __name__ == "__main__":
    # Creating Class instances
    data_cleaner = DataCleaning()
    data_extractor = DataExtractor()
    data_loader = DataLoader(config('local_db_path'))
    db_connector = DatabaseConnector(config('rds_db_path'))

    # Retive raw data
    raw_store_data =    data_extractor.retrieve_stores_data(config('store_endpoint_template'), data_extractor.list_number_of_stores(config('number_stores_endpoint')))
    raw_orders_data =   data_extractor.read_rds_table(db_connector, db_connector.list_db_tables()[2])
    raw_user_data =     data_extractor.read_rds_table(db_connector, db_connector.list_db_tables()[1])
    raw_date_data =     data_extractor.extract_from_s3(config('s3_date_address'))
    raw_products_data = data_extractor.extract_from_s3(config('s3_products_address'))
    raw_card_data =     data_extractor.retrieve_pdf_data(config('pdf_path'))
    print("Raw data extracted")

    # Clean raw data
    cleaned_orders_data =       data_cleaner.clean_orders_data(raw_orders_data)
    cleaned_user_data =         data_cleaner.clean_user_data(raw_user_data)
    cleaned_date_data =         data_cleaner.clean_date_data(raw_date_data)
    cleaned_products_data =     data_cleaner.clean_products_data(raw_products_data)
    cleaned_store_data =        data_cleaner.clean_store_data(raw_store_data)
    cleaned_card_data =         data_cleaner.clean_card_data(raw_card_data)
    print("Raw data cleaned")

    # Load clean data into PostgreSQL tables
    data_loader.upload_and_configure_table(cleaned_orders_data,   'orders_table',      data_loader.orders_column_types(),   primary_key=None,           foreign_keys=data_loader.orders_foreign_keys())
    data_loader.upload_and_configure_table(cleaned_user_data,     'dim_users',         data_loader.user_column_types(),     primary_key='user_uuid',    foreign_keys=None)
    data_loader.upload_and_configure_table(cleaned_date_data,     'dim_date_times',    data_loader.date_column_types(),     primary_key='date_uuid',    foreign_keys=None)
    data_loader.upload_and_configure_table(cleaned_products_data, 'dim_products',      data_loader.products_column_types(), primary_key='product_code', foreign_keys=None)
    data_loader.upload_and_configure_table(cleaned_store_data,    'dim_store_details', data_loader.store_column_types(),    primary_key='store_code',   foreign_keys=None)
    data_loader.upload_and_configure_table(cleaned_card_data,     'dim_card_details',  data_loader.card_column_types(),     primary_key='card_number',  foreign_keys=None)
    print("Cleaned data loaded to PostgreSQL tables")

