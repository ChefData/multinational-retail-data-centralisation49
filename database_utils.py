#%%
import yaml
from sqlalchemy import create_engine, inspect, pool
from sqlalchemy.exc import SQLAlchemyError
from data_cleaning import DataCleaning
from data_extraction import DataExtractor

class DatabaseConnector:
    """
    A class for connecting to databases using SQLAlchemy.

    Attributes:
        - db_engine: SQLAlchemy engine for the main database connection.
    """

    def __init__(self, db_creds_file):
        """
        Initialize the DatabaseConnector.

        Parameters:
            - db_creds_file (str): Path to the YAML file containing database credentials.
        """
        self.db_creds_file = db_creds_file
        self.db_engine = self.init_db_engine()

    def read_db_creds(self):
        """
        Read database credentials from the YAML file.

        Returns:
            dict: Database credentials.
        
        Raises:
            FileNotFoundError: If the YAML file is not found.
            yaml.YAMLError: If there is an issue loading YAML from the file.
        """
        try:
            with open(self.db_creds_file, 'r') as file:
                return yaml.safe_load(file)
        except FileNotFoundError as e:
            raise FileNotFoundError(f"Warning: database credentials file '{self.db_creds_file}' not found: {e}")
        except yaml.YAMLError as e:
            raise yaml.YAMLError(f"Error: Unable to load YAML from '{self.db_creds_file}': {e}")

    def create_db_url(self):
        """
        Creates a database URL from the provided credentials.

        Returns:
            str: Database URL for SQLAlchemy engine.

        Raises:
            ValueError: If database credentials are missing or incomplete.
        """
        try:
            db_creds = self.read_db_creds()
            required_keys = ['DRIVER', 'USER', 'PASSWORD', 'HOST', 'PORT', 'DATABASE']
            if any(key not in db_creds for key in required_keys):
                raise ValueError("Error: Database credentials are missing or incomplete.")
            return f"{db_creds['DRIVER']}://{db_creds['USER']}:{db_creds['PASSWORD']}@{db_creds['HOST']}:{db_creds['PORT']}/{db_creds['DATABASE']}"
        except ValueError as e:
            raise ValueError(f"Error creating database URL: {e}")

    def init_db_engine(self):
        """
        Reads database credentials, initializes, and returns a SQLAlchemy database engine.

        Returns:
            sqlalchemy.engine.base.Engine: SQLAlchemy database engine.

        Raises:
            SQLAlchemyError: If there is an error initializing the database engine.
        """
        try:
            db_url = self.create_db_url()
            engine_params = {'pool_size': 5, 'poolclass': pool.QueuePool}
            return create_engine(db_url, **engine_params)
        except SQLAlchemyError as e:
            raise SQLAlchemyError(f"Error initializing database engine: {e}")

    def list_db_tables(self):
        """
        Get a list of tables in the main database.

        Returns:
            list: List of table names in the database.

        Raises:
            SQLAlchemyError: If there is an error listing database tables.
        """
        try:
            with self.db_engine.connect() as connection:
                inspector = inspect(connection)
                return inspector.get_table_names()
        except SQLAlchemyError as e:
            raise SQLAlchemyError(f"Error listing database tables: {e}")

    def upload_to_db(self, df, table_name):
        """
        Upload a DataFrame to a specified table in the local database.

        Parameters:
            - df (pd.DataFrame): DataFrame to upload.
            - table_name (str): Name of the table in the database.

        Raises:
            SQLAlchemyError: If there is an error uploading the DataFrame to the database.
        """
        try:
            with self.db_engine.connect() as connection:
                df.to_sql(table_name, connection, if_exists='replace', index=False)
        except SQLAlchemyError as e:
            raise SQLAlchemyError(f"Error uploading DataFrame to database: {e}")


if __name__ == "__main__":
    # Creating Class instances
    data_cleaner = DataCleaning()
    data_extractor = DataExtractor()
    db_connector = DatabaseConnector('do_not_track/db_creds.yaml')
    pg_connector = DatabaseConnector('do_not_track/pg_creds.yaml')

    # Reading and printing database credentials
    db_creds = db_connector.read_db_creds()
    print("Database Credentials:", db_creds)

    # Initializing and printing the database engine
    db_engine = db_connector.init_db_engine()
    print("Database Engine:", db_engine)

    # Listing and printing all tables in the database
    db_tables = db_connector.list_db_tables()
    print("Database Tables:", db_tables)
    
    # Using list_db_tables to get the name of the table containing user data
    user_data_table = db_connector.list_db_tables()[1]
    
    # Using read_rds_table to extract the table containing user data
    raw_data = data_extractor.read_rds_table(db_connector, user_data_table)
    
    # Printing the original DataFrame
    print("User Data DataFrame:")
    print(raw_data)
    print(raw_data.info())

    # Cleaning user data
    cleaned_data = data_cleaner.clean_user_data(raw_data)
    
    # Printing the cleaned DataFrame
    print("Cleaned User Data:")
    print(cleaned_data)
    print(cleaned_data.info())

    # Uploading DataFrame to a specified table
    table_name_to_upload = 'dim_users'
    pg_connector.upload_to_db(cleaned_data, table_name_to_upload)
    pg_engine = pg_connector.init_db_engine()
    print(f"Data uploaded to the '{table_name_to_upload}' table in the 'sales_data' PostgreSQL database.\nPostgreSQL Database Engine: '{pg_engine}'")