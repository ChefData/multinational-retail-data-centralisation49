import pandas as pd
import tabula

class DataExtractor:
    """
    A class for extracting data from a database using a provided DatabaseConnector.
    """

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
            raise RuntimeError("Error initializing database engine. Check your connection settings.") from e

        # List tables in the database
        tables = db_connector.list_db_tables() 
        
        # Check if the specified table exists
        if table_name not in tables: 
            raise ValueError(f"Table '{table_name}' not found in the database. Available tables: {tables}")
        
        # Read the table into a Pandas DataFrame
        with db_engine.connect() as connection:
            df = pd.read_sql_table(table_name, connection)
        
        return df
    
    def retrieve_pdf_data(self):
        pdf_path = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
        dfs = tabula.read_pdf(pdf_path, pages='all')
