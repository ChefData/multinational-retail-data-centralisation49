import logging
import yaml
from sqlalchemy import create_engine, inspect, pool
from sqlalchemy.exc import SQLAlchemyError
from data_cleaning import DataCleaning
from data_extraction import DataExtractor


class DatabaseConnector:
    """
    A class for connecting to databases using SQLAlchemy.

    Attributes:
    - engine: SQLAlchemy engine for the main database connection.
    """

    def __init__(self, db_creds_file='do_not_track/db_creds.yaml', pg_creds_file='do_not_track/pg_creds.yaml'):
        """
        Initialize the DatabaseConnector.

        Parameters:
        - db_creds_file (str): Path to the YAML file containing database credentials.
        - local_creds_file (str): Path to the YAML file containing local database credentials.
        """
        self.db_creds_file = db_creds_file
        self.pg_creds_file = pg_creds_file
        self.db_engine = self.init_db_engine()
        self.pg_engine = self.init_pg_engine()
        self.logger = self.setup_logging()

    def setup_logging(self):
        logging.basicConfig(level=logging.INFO)
        return logging.getLogger(__name__)

    def read_db_creds(self):
        """
        Read AWS database credentials from the YAML file.

        Parameters:
        - db_creds_file (str): Path to the YAML file containing AWS database credentials.

        Returns:
        - dict: AWS database credentials.
        """
        try:
            with open(self.db_creds_file, 'r') as file:
                db_creds = yaml.safe_load(file)
            return db_creds
        except FileNotFoundError as e:
            self.logger.error(f"Error: AWS database credentials file '{self.db_creds_file}' not found.")
            raise FileNotFoundError(f"Error: AWS database credentials file '{self.db_creds_file}' not found.") from e
        except yaml.YAMLError as e:
            self.logger.error(f"Error: Unable to load YAML from '{self.db_creds_file}': {e}")
            raise ValueError(f"Error: Unable to load YAML from '{self.db_creds_file}': {e}") from e

    def read_pg_creds(self):
        """
        Read PostgreSQL database credentials from the YAML file.

        Parameters:
        - pg_creds_file (str): Path to the YAML file containing PostgreSQL database credentials.

        Returns:
        - dict: PostgreSQL database credentials.
        """
        try:
            with open(self.pg_creds_file, 'r') as file:
                pg_creds = yaml.safe_load(file)
            return pg_creds
        except FileNotFoundError as e:
            self.logger.error(f"Error: PostgreSQL database credentials file '{self.pg_creds_file}' not found.")
            raise FileNotFoundError(f"Error: PostgreSQL database credentials file '{self.pg_creds_file}' not found.") from e
        except yaml.YAMLError as e:
            self.logger.error(f"Error: Unable to load YAML from '{self.pg_creds_file}': {e}")
            raise ValueError(f"Error: Unable to load YAML from '{self.pg_creds_file}': {e}") from e

    def create_db_url(self):
        """
        Creates a AWS database URL from the provided credentials.

        Returns:
            str: AWS database URL for SQLAlchemy engine.
        """
        try:
            db_creds = self.read_db_creds()
            if any(key not in db_creds for key in ['DRIVER', 'USER', 'PASSWORD', 'HOST', 'PORT', 'DATABASE']):
                self.logger.error("Error: AWS database credentials are missing or incomplete.")
                raise ValueError("Error: AWS database credentials are missing or incomplete.")
            db_url = f"{db_creds['DRIVER']}://{db_creds['USER']}:{db_creds['PASSWORD']}@{db_creds['HOST']}:{db_creds['PORT']}/{db_creds['DATABASE']}"
            return db_url
        except Exception as e:
            self.logger.error(f"Error creating AWS database URL: {e}")
            raise ValueError(f"Error creating AWS database URL: {e}")
    
    def create_pg_url(self):
        """
        Creates a PostgreSQL database URL from the provided credentials.

        Returns:
            str: Database URL for SQLAlchemy engine.
        """
        try:
            pg_creds = self.read_pg_creds()
            if any(key not in pg_creds for key in ['DRIVER', 'USER', 'PASSWORD', 'HOST', 'PORT', 'DATABASE']):
                self.logger.error("Error: PostgreSQL database credentials are missing or incomplete.")
                raise ValueError("Error: PostgreSQL database credentials are missing or incomplete.")
            pg_url = f"{pg_creds['DRIVER']}://{pg_creds['USER']}:{pg_creds['PASSWORD']}@{pg_creds['HOST']}:{pg_creds['PORT']}/{pg_creds['DATABASE']}"
            return pg_url
        except Exception as e:
            self.logger.error(f"Error creating PostgreSQL database URL: {e}")
            raise ValueError(f"Error creating PostgreSQL database URL: {e}")

    def init_db_engine(self):
        """
        Reads AWS database credentials, initializes, and returns a SQLAlchemy database engine.

        Returns:
            sqlalchemy.engine.base.Engine: SQLAlchemy database engine.
        """
        try:
            db_url = self.create_db_url()
            if db_url is not None:
                pool_size = 5
                db_engine = create_engine(db_url, pool_size=pool_size, poolclass=pool.QueuePool)
                db_engine.execution_options(isolation_level='AUTOCOMMIT').connect()
                return db_engine
        except SQLAlchemyError as e:
            self.logger.error(f"Error initializing AWS database engine: {e}")
            raise ValueError(f"Error initializing AWS database engine: {e}")

    def init_pg_engine(self):
        """
        Reads PostgreSQL database credentials, initializes, and returns a SQLAlchemy database engine.

        Returns:
            sqlalchemy.engine.base.Engine: SQLAlchemy database engine.
        """
        try:
            pg_url = self.create_pg_url()
            if pg_url is not None:
                pool_size = 5
                pg_engine = create_engine(pg_url, pool_size=pool_size, poolclass=pool.QueuePool)
                pg_engine.execution_options(isolation_level='AUTOCOMMIT').connect()
                return pg_engine
        except SQLAlchemyError as e:
            self.logger.error(f"Error initializing PostgreSQL database engine: {e}")
            raise ValueError(f"Error initializing PostgreSQL database engine: {e}")

    def list_db_tables(self):
        """
        Get a list of tables in the main database.

        Returns:
            list: List of table names in the database.
        """
        try:
            with self.db_engine.connect() as connection:
                inspector = inspect(self.db_engine)
                table_names = inspector.get_table_names()
            return table_names
        except SQLAlchemyError as e:
            self.logger.error(f"Error listing AWS database tables: {e}")
            return []

    def upload_to_pg(self, df, table_name):
        """
        Upload a DataFrame to a specified table in the local database.

        Parameters:
        - df (pd.DataFrame): DataFrame to upload.
        - table_name (str): Name of the table in the database.
        """
        try:
            df.to_sql(table_name, self.pg_engine, if_exists='replace', index=False)
        except Exception as e:
            print(f"Error uploading DataFrame to PostgreSQL database: {e}")

    def close_db_connection(self):
        """
        Close the SQLAlchemy database connection.
        """
        try:
            if self.db_engine is not None:
                self.db_engine.dispose()
                self.logger.info("AWS database connection closed.")
        except SQLAlchemyError as e:
            self.logger.error(f"Error closing AWS database connection: {e}")

    def close_pg_connection(self):
        """
        Close the SQLAlchemy PostgreSQL database connection.
        """
        try:
            if self.pg_engine is not None:
                self.pg_engine.dispose()
                self.logger.info("PostgreSQL database connection closed.")
        except SQLAlchemyError as e:
            self.logger.error(f"Error closing PostgreSQL database connection: {e}")

if __name__ == "__main__":
    # Creating Class instances
    data_cleaner = DataCleaning()
    data_extractor = DataExtractor()
    db_connector = DatabaseConnector()
    
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

    # Cleaning user data
    cleaned_data = data_cleaner.clean_user_data(raw_data)
    
    # Uploading DataFrame to a specified table
    table_name_to_upload = 'dim_users'
    db_connector.upload_to_pg(cleaned_data, table_name_to_upload)
    print(f"Data uploaded to the '{table_name_to_upload}' table in the 'sales_data' database.")
    
    # Close the database connection
    db_connector.close_db_connection()
    db_connector.close_pg_connection()
