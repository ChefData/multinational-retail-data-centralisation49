# Import necessary modules for handling AWS services, PDF extraction, data manipulation, and HTTP requests
from botocore.exceptions import NoCredentialsError, ClientError
from decouple import config
import boto3, json, pandas as pd, requests, tabula


class DataExtractor:
    """
    A class for extracting data from various sources such as databases, PDFs, APIs, and S3.
    """

    def __init__(self):
        """
        Initialises a DataExtractor object.
        
        Attributes:
        - header (dict): API key header.
        """
        # Constructor to initialise the API key header
        self.header = {'x-api-key': config('api_key')}

    @staticmethod
    def read_rds_table(db_connector, table_name: str) -> pd.DataFrame:
        """
        Reads a table from a relational database and returns the data as a Pandas DataFrame.

        Parameters:
        - db_connector: An object with an '_init_db_engine' method for initialising a database engine.
        - table_name (str): Name of the table in the database.

        Returns:
        - pd.DataFrame: Data from the specified table.
        
        Raises:
        - RuntimeError: If there is an error initialising the database engine.
        - ValueError: If the specified table does not exist in the database.
        """
        # Get the database engine from the DatabaseConnector
        try:
            db_engine = db_connector._init_db_engine()
        # Handle RuntimeError error
        except RuntimeError as error:
            raise RuntimeError(f"Error initialising database engine. Check your connection settings: {error}")

        # List tables in the database
        tables = db_connector.list_db_tables() 
        
        # Check if the specified table exists 
        if table_name not in tables: 
            raise ValueError(f"Table '{table_name}' not found in the database. Available tables: {tables}")
        
        # Read the table into a Pandas DataFrame
        with db_engine.connect() as connection:
            user_data_df = pd.read_sql_table(table_name, connection, index_col='index')
        return user_data_df
    
    @staticmethod
    def retrieve_pdf_data(pdf_link) -> pd.DataFrame:
        """
        Retrieves data from a PDF link and returns it as a Pandas DataFrame.

        Parameters:
        - pdf_link (str): Link to the PDF file.

        Returns:
        - pd.DataFrame: Data from the PDF.

        Raises:
        - FileNotFoundError: If the PDF file is not found.
        - RuntimeError: If there is an error processing the PDF.
        """
        try:
            # Read the PDF into a list of DataFrames
            pdf_data = tabula.read_pdf(pdf_link, pages='all', multiple_tables=True, lattice=True)
            
            # Concatenate the list of DataFrames into a single DataFrame
            card_data_df = pd.concat(pdf_data)
            return card_data_df
        except FileNotFoundError:
            raise FileNotFoundError(f"The PDF file was not found @ link: '{pdf_link}'")
        except RuntimeError as error:
            raise RuntimeError(f"An error occurred while processing the PDF: {error}")

    def list_number_of_stores(self, number_stores_endpoint):
        """
        Retrieves the number of stores from an API endpoint.

        Parameters:
        - number_stores_endpoint (str): API endpoint for retrieving the number of stores.

        Returns:
        - int: Number of stores.

        Raises:
        - requests.exceptions.RequestException: If there is an error retrieving the number of stores.
        """
        try:
            with requests.Session() as session:
                # Send a GET request to the API to retrieve information
                response = session.get(number_stores_endpoint, headers=self.header)
                # Raise an HTTPError for bad responses (4xx or 5xx)
                response.raise_for_status()
                # Access the response data as JSON
                number_of_stores = response.json().get('number_stores')
                return number_of_stores
        except requests.exceptions.RequestException as error:
            raise requests.exceptions.RequestException(f"Error retrieving number of stores: {error} \nStatus code: {response.status_code} \nResponse Text: {response.text}")

    def retrieve_stores_data(self, store_endpoint, number_of_stores) -> int:
        """
        Retrieves store data from an API endpoint for a given number of stores and returns it as a Pandas DataFrame.

        Parameters:
        - store_endpoint (str): API endpoint for retrieving store data.
        - number_of_stores (int): Number of stores to retrieve.

        Returns:
        - pd.DataFrame: Store data.

        Raises:
        - requests.exceptions.RequestException: If there is an error retrieving store data.
        """
        try:
            # Send a GET request to the API endpoint for each store and access the response data as JSON
            all_stores_data = [requests.get(store_endpoint.format(store_number), headers=self.header).json()
                               for store_number in range(0, number_of_stores)]
            # Create a DataFrame from the list of store data
            df = pd.DataFrame(all_stores_data)
            return df
        except requests.exceptions.RequestException as error:
            raise requests.exceptions.RequestException(f"Error retrieving store data: {error}")

    @staticmethod
    def extract_from_s3(s3_address) -> pd.DataFrame:
        """
        Extracts data from an S3 bucket based on the provided S3 address and returns it as a Pandas DataFrame.

        Parameters:
        - s3_address (str): S3 address specifying the bucket and key.

        Returns:
        - pd.DataFrame: Data from the S3 bucket.

        Raises:
        - NoCredentialsError: If AWS credentials are not found.
        - ClientError: If there is an error related to AWS S3.
        - ValueError: If the file format in the S3 address is not supported.
        """
        try:
            # Create connection to S3 using default config and all buckets within S3. 's3' is a key word.
            s3 = boto3.client('s3')
            
            # Extract bucket and key from S3 address
            bucket, key = s3_address.replace('s3://', '').split('/', 1)

           # Get object and file (key) from bucket
            response = s3.get_object(Bucket= bucket, Key= key) 

            # Create a DataFrame from S3 file. 'Body' is a key word
            data = response['Body']

            # Check the file format based on the file extension
            if s3_address.endswith('.csv'):
                df = pd.read_csv(data)
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
        
        except NoCredentialsError as error:
            raise NoCredentialsError(f"AWS credentials not found. Please configure your credentials: {error}")
        except ClientError as error:
            if error.response['Error']['Code'] == 'NoSuchBucket':
                raise ClientError(f"The specified bucket does not exist: {error}")
            else:
                raise ClientError(f"An error occurred: {error}")