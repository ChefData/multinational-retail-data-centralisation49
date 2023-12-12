import pandas as pd
import tabula
import requests


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
