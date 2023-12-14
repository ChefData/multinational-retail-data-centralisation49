from data_cleaning import DataCleaning
from data_extraction import DataExtractor
from database_utils import DatabaseConnector

if __name__ == "__main__":
    # Creating Class instances
    data_cleaner = DataCleaning()
    data_extractor = DataExtractor()
    pg_connector = DatabaseConnector('do_not_track/pg_creds.yaml')

    '''
    You will need to be logged into the AWS CLI before you retrieve the data from the bucket.
    1. Open a terminal or command prompt on your local machine
    2. Run the following command to start the AWS CLI configuration process: 
        aws configure
    3. You will be prompted to enter the following information:
        * AWS Access Key ID: Enter the access key ID you obtained during the access key generation process
        * AWS Secret Access Key: Enter the secret access key corresponding to the access key ID you provided
        * Default region name: Specify the default AWS region you want to use for AWS CLI commands. 
            In our case, we will use eu-west-1, as this region is geographically close to the UK.
        * Default output format: Choose the default output format for AWS CLI command results. 
            You can enter JSON, text, table, or YAML. The default format is typically JSON, which provides 
            machine-readable output. If you enter nothing (press Enter) it will default to JSON.
    4. After entering the required information, press Enter
    5. To verify that the configuration was successful, run the following command: aws configure list. 
        This command will display the current configuration settings, including the access key ID, 
        secret access key (partially masked), default region, and default output format. 
        Make sure the displayed values match the credentials you provided during the configuration.
    '''

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
    pg_engine = pg_connector.init_db_engine()
    print(f"Data uploaded to the '{table_name_to_upload}' table in the 'sales_data' PostgreSQL database.\nPostgreSQL Database Engine: '{pg_engine}'")