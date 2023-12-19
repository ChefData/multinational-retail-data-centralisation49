# Import necessary modules from SQLAlchemy, urllib, and PyYAML for database operations
from sqlalchemy import create_engine, inspect, URL
from sqlalchemy.exc import SQLAlchemyError
import psycopg2
import urllib.parse
import yaml


# Define a class for handling database connections and operations
class DatabaseConnector:
    """
    A class for connecting to a database, reading database credentials from a YAML file,
    creating a database URL, initialising a SQLAlchemy engine, and performing database operations.

    Attributes:
    - db_creds_file (str): Path to the YAML file containing database credentials.
    - db_engine (sqlalchemy.engine.Engine): SQLAlchemy engine for database operations.

    Private Methods:
    - __init__(self, db_creds_file: str) -> None:
        Initialises a DatabaseConnector object.

    - __read_db_creds(self) -> dict:
        Reads and returns the database credentials from the specified YAML file.

    - __create_db_url(self) -> URL:
        Creates a SQLAlchemy database URL based on the provided database credentials.

    Protected Methods:
    - _init_db_engine(self) -> create_engine:
        Initialises and returns a SQLAlchemy engine using the database URL.
    
    Public Methods:
    - list_db_tables(self) -> list:
        Lists the names of tables in the connected database.

    - upload_to_db(self, df: pd.DataFrame, table_name: str) -> None:
        Uploads a Pandas DataFrame to the specified table in the connected database.

    """

    def __init__(self, db_creds_file: str) -> None:
        """
        Initialises a DatabaseConnector object.

        Parameters:
        - db_creds_file (str): Path to the YAML file containing database credentials.
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

    def __create_db_url(self) -> URL:
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
            db_url = self.__create_db_url()
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

    def upload_to_db(self, df, table_name) -> None:
        """
        Uploads a Pandas DataFrame to the specified table in the connected database.

        Parameters:
        - df (pd.DataFrame): DataFrame to be uploaded.
        - table_name (str): Name of the table in the database.

        Raises:
        - SQLAlchemyError: If there is an error uploading the DataFrame to the database.
        """
        try:
            # Establish a connection to the database engine
            with self.db_engine.connect() as connection:
                # Use the to_sql method to upload the DataFrame to the specified table
                df.to_sql(table_name, connection, if_exists='replace', index=False)
        # Handle any errors that may occur during DataFrame upload
        except SQLAlchemyError as error:
            raise SQLAlchemyError(f"Error uploading DataFrame to database: {error}")

    def __create_psycopg2_url(self) -> str:
        """
        Creates a PostgreSQL connection URL based on the provided database credentials.

        Returns:
        - str: PostgreSQL connection URL.

        Raises:
        - ValueError: If required database credentials are missing or incomplete.
        """
        try:
            # Read database credentials from the YAML file
            db_creds = self.__read_db_creds()
            # Check if all required keys are present in the credentials
            required_keys = ['USER', 'PASSWORD', 'HOST', 'PORT', 'DATABASE']
            if any(key not in db_creds for key in required_keys):
                raise ValueError("Error: Database credentials are missing or incomplete.")
            # Create a connection string
            conn_string = (
                f"user={db_creds['USER']} "
                f"password={urllib.parse.quote_plus(db_creds['PASSWORD'])} "
                f"host={db_creds['HOST']} "
                f"port={db_creds['PORT']} "
                f"dbname={db_creds['DATABASE']}"
            )
            return conn_string
        except ValueError as error:
            raise ValueError(f"Error creating database connection string: {error}")

    def cast_data_types(self, table_name, column_types) -> None:
        """
        Casts the data types of columns in a PostgreSQL table based on the provided dictionary of column types.

        Parameters:
        - table_name (str): The name of the PostgreSQL table.
        - column_types (dict): A dictionary where keys are column names and values are the desired data types.
        """
        # Initialize max_lengths dictionary
        max_lengths = {}
        db_url = self.__create_psycopg2_url()
        # Connect to the PostgreSQL database
        with psycopg2.connect(db_url) as conn:
            with conn.cursor() as cur:
                for column_name, data_type in column_types.items():
                    if data_type == 'VARCHAR':
                        # Construct the query to find the maximum length
                        query = f"SELECT MAX(CHAR_LENGTH(CAST({column_name} AS VARCHAR))) FROM {table_name};"
                        # Execute the query
                        cur.execute(query)
                        # Fetch the result
                        max_length = cur.fetchone()[0]
                        # Update max_lengths dictionary
                        max_lengths[column_name] = max_length
                        # Construct the ALTER TABLE query
                        alter_query = f"ALTER TABLE {table_name} ALTER COLUMN {column_name} TYPE {data_type}({max_length});"
                    else:
                        # For non-VARCHAR columns
                        alter_query = f"ALTER TABLE {table_name} ALTER COLUMN {column_name} TYPE {data_type} USING {column_name}::{data_type};"
                    # Execute the ALTER TABLE query
                    cur.execute(alter_query)
