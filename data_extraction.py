import pandas as pd
import tabula
import requests
import boto3
from botocore.exceptions import NoCredentialsError, ClientError
import json


class DataExtractor:
    """
    A class for extracting data from a database using a provided DatabaseConnector.
    """
    def __init__(self):
        self.header = None

    def read_rds_table(self, db_connector, table_name: str) -> pd.DataFrame:
        """
        Read a table from a database using the provided DatabaseConnector.

        Parameters:
            - db_connector: An instance of a DatabaseConnector.
            - table_name: Name of the table to read.

        Returns:
            - Pandas DataFrame containing the data from the specified table.

        Raises:
            - RuntimeError: If there is an error initializing the database engine.
            - ValueError: If the specified table does not exist in the database.

        """
        # Get the engine from the DatabaseConnector
        try:
            db_engine = db_connector.init_db_engine()
        except Exception as e:
            raise RuntimeError("Error initializing database engine. Check your connection settings: {e}")

        # List tables in the database
        tables = db_connector.list_db_tables() 
        
        # Check if the specified table exists
        if table_name not in tables: 
            raise ValueError(f"Table '{table_name}' not found in the database. Available tables: {tables}")
        
        # Read the table into a Pandas DataFrame
        with db_engine.connect() as connection:
            user_data_df = pd.read_sql_table(table_name, connection, index_col='index')
        return user_data_df
    
    def retrieve_pdf_data(self, pdf_link):
        """
        Retrieve data from a PDF file.

        Parameters:
        - pdf_link (str): The file path or URL of the PDF.

        Returns:
        - pandas.DataFrame: A DataFrame containing the extracted data from the PDF.

        Raises:
        - FileNotFoundError: If the specified PDF file is not found.
        - RuntimeError: If an unexpected error occurs during PDF processing.
        """
        try:
            # Read pdf into list of DataFrame
            pdf_data = tabula.read_pdf(pdf_link, pages='all', multiple_tables=True, lattice=True)
            
            # Concatenate list of DataFrame into single dataframe
            card_data_df = pd.concat(pdf_data)
            return card_data_df
        except FileNotFoundError:
            raise FileNotFoundError(f"The PDF file was not found @ link: '{pdf_link}'")
        except Exception as e:
            raise RuntimeError(f"An error occurred while processing the PDF: {e}")

    def set_api_key(self, api_key):
        self.header = {'x-api-key': api_key}

    def list_number_of_stores(self, number_stores_endpoint):
        try:
            # Send a GET request to the API to retrieve information
            response = requests.get(number_stores_endpoint, headers=self.header)
            # Raise an HTTPError for bad responses (4xx or 5xx)
            response.raise_for_status()
            # Access the response data as JSON
            number_of_stores = response.json().get('number_stores')
            return number_of_stores
        except requests.exceptions.RequestException as e:
            raise requests.exceptions.RequestException(f"Error retrieving number of stores: {e} \nStatus code: {response.status_code} \nResponse Text: {response.text}")
            
    def retrieve_stores_data(self, store_endpoint, number_of_stores):
        try:
            all_stores_data = []
            for store_number in range(1, number_of_stores):
                # Send a GET request to the API endpoint for each store
                response = requests.get(store_endpoint.format(store_number), headers=self.header)
                # Raise an HTTPError for bad responses (4xx or 5xx)
                response.raise_for_status()  
                # Access the response data as JSON and Append to the list
                store_data = response.json()
                all_stores_data.append(store_data)
            # Create a DataFrame from the list of store data
            df = pd.DataFrame(all_stores_data)
            return df
        except requests.exceptions.RequestException as e:
            raise requests.exceptions.RequestException(f"Error retrieving store data: {e}")
        
    def extract_from_s3(self, s3_address):
        try:
            # Create connection to S3 using default config and all buckets within S3. 's3' is a key word.
            s3 = boto3.client('s3')
            
            # Extract bucket and key from S3 address
            bucket, key = s3_address.replace('s3://', '').split('/', 1)

           # Get object and file (key) from bucket
            response = s3.get_object(Bucket= bucket, Key= key) 

            # Create a DataFrame from S3 csv file. 'Body' is a key word
            data = response['Body']

            # Check if the data is in CSV format based on file extension
            if s3_address.endswith('.csv'):
                df = pd.read_csv(data)
            
            # Check if the data is in JSON format based on file extension
            elif s3_address.endswith('.json'):
                # Read the content of the StreamingBody as bytes
                data_read = data.read()
                # Decode the bytes into a string (assuming it's in utf-8 encoding)
                data_str = data_read.decode('utf-8')
                # Parse the string as JSON and create a DataFrame
                df = pd.DataFrame(json.loads(data_str))

            # Handle other formats or raise an error if needed
            else:
                raise ValueError(f"Unsupported file format for S3 address: {s3_address}")
            return df
        
        except NoCredentialsError as e:
            raise NoCredentialsError(f"AWS credentials not found. Please configure your credentials: {e}")
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchBucket':
                raise ClientError("The specified bucket does not exist: {e}")
            else:
                raise ClientError("An error occurred: {e}")