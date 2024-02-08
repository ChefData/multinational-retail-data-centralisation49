# Import necessary modules from psycopg2, SQLAlchemy, urllib, and PyYAML for database operations
from sqlalchemy import create_engine, inspect, URL
from sqlalchemy.exc import SQLAlchemyError
import urllib.parse, yaml


# Define a class for handling database connections and operations
class DatabaseConnector:
    """
    A class for connecting to a database, reading database credentials from a YAML file,
    initialising a SQLAlchemy engine, performing database operations, 
    interacting with a PostgreSQL database, providing methods for
    creating connection URLs, casting data types, adding primary keys, and
    adding foreign keys to tables.
    """

    def __init__(self, db_creds_file: str) -> None:
        """
        Initialises a DatabaseConnector object.

        Parameters:
        - db_creds_file (str): Path to the YAML file containing database credentials.
        
        Attributes:
        - db_engine (sqlalchemy.engine.Engine): SQLAlchemy engine for database operations.
        """
        # Constructor that takes the path to a YAML file containing database credentials
        self.db_creds_file = db_creds_file
        # Initialise the database engine upon instantiation
        self.db_engine = self._init_db_engine()

    def __read_db_creds(self) -> dict:
        """
        Reads and returns the database credentials from the specified YAML file.

        Returns:
        - dict: Database credentials.

        Raises:
        - FileNotFoundError: If the specified credentials file is not found.
        - yaml.YAMLError: If there is an error loading YAML from the file.
        """
        try:
            # Read the database credentials from the YAML file
            with open(self.db_creds_file, 'r') as file:
                return yaml.safe_load(file)
        # Handle file not found error
        except FileNotFoundError as error:
            raise FileNotFoundError(f"Error: database credentials file '{self.db_creds_file}' not found: {error}")
        # Handle YAML parsing error
        except yaml.YAMLError as error:
            raise yaml.YAMLError(f"Error: Unable to load YAML from '{self.db_creds_file}': {error}")

    def _create_db_url(self) -> URL:
        """
        Creates a SQLAlchemy database URL based on the provided database credentials.

        Returns:
        - sqlalchemy.engine.url.URL: SQLAlchemy database URL.

        Raises:
        - ValueError: If required database credentials are missing or incomplete.
        """
        try:
            # Read database credentials from the YAML file
            db_creds = self.__read_db_creds()
            # Check if all required keys are present in the credentials
            required_keys = ['DRIVER', 'USER', 'PASSWORD', 'HOST', 'PORT', 'DATABASE']
            if any(key not in db_creds for key in required_keys):
                raise ValueError("Error: Database credentials are missing or incomplete.")
            # Create a SQLAlchemy URL object
            url_object = URL.create(
                drivername=db_creds['DRIVER'],
                username=db_creds['USER'],
                password=urllib.parse.quote_plus(db_creds['PASSWORD']),
                host=db_creds['HOST'],
                port=db_creds['PORT'],
                database=db_creds['DATABASE'],
            )
            return url_object
        # Handle any errors that may occur during URL creation
        except ValueError as error:
            raise ValueError(f"Error creating database URL: {error}")

    def _init_db_engine(self) -> create_engine:
        """
        Initialises and returns a SQLAlchemy engine using the database URL.

        Returns:
        - sqlalchemy.engine.Engine: SQLAlchemy engine for database operations.

        Raises:
        - SQLAlchemyError: If there is an error initialising the database engine.
        """
        try:
            # Create a database URL
            db_url = self._create_db_url()
            # Initialise the database engine
            engine = create_engine(db_url)
            return engine
        # Handle any errors that may occur during engine initialisation
        except SQLAlchemyError as error:
            raise SQLAlchemyError(f"Error initialising database engine: {error}")

    def list_db_tables(self) -> list:
        """
        Lists the names of tables in the connected database.

        Returns:
        - list: Names of tables in the database.

        Raises:
        - SQLAlchemyError: If there is an error listing database tables.
        """
        try:
            # Establish a connection to the database engine
            with self.db_engine.connect() as connection:
                # Use the inspector to get a list of table names
                inspector = inspect(connection)
                table_names = inspector.get_table_names()
                return table_names
        # Handle any errors that may occur during table listing
        except SQLAlchemyError as error:
            raise SQLAlchemyError(f"Error listing database tables: {error}")