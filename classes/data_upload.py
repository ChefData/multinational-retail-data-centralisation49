from classes.database_utils import DatabaseConnector
from sqlalchemy.exc import SQLAlchemyError
from typing import Dict, Optional
import pandas as pd, psycopg2


class DataLoader:
    """
    DataLoader class for uploading and configuring tables in a PostgreSQL database.
    """

    def __init__(self, db_path: str) -> None:
        """
        Initialises a DataLoader instance.

        Parameters:
        - db_path (str): Path to the PostgreSQL database.

        Attributes:
        - pg_connector (DatabaseConnector): Instance of DatabaseConnector for database operations.
        """
        self.pg_connector = DatabaseConnector(db_path)
        self.db_url = str(self.pg_connector._create_db_url())

    def __upload_to_db(self, df, table_name) -> None:
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
            with self.pg_connector.db_engine.connect() as connection:
                # Use the to_sql method to upload the DataFrame to the specified table
                df.to_sql(table_name, connection, if_exists='replace', index=False)
        # Handle any errors that may occur during DataFrame upload
        except SQLAlchemyError as error:
            raise SQLAlchemyError(f"Error uploading DataFrame to database: {error}")

    def __cast_data_types(self, table_name, column_types) -> None:
        """
        Casts the data types of columns in a PostgreSQL table based on the provided dictionary of column types.

        Parameters:
        - table_name (str): The name of the PostgreSQL table.
        - column_types (dict): A dictionary where keys are column names and values are the desired data types.
        """
        # Initialize max_lengths dictionary
        max_lengths = {}
        # Connect to the PostgreSQL database
        with psycopg2.connect(self.db_url) as conn:
            with conn.cursor() as cur:
                for column_name, data_type in column_types.items():
                    if data_type == 'VARCHAR(?)':
                        # Construct the query to find the maximum length
                        query = f"SELECT MAX(CHAR_LENGTH(CAST({column_name} AS VARCHAR))) FROM {table_name};"
                        # Execute the query
                        cur.execute(query)
                        # Fetch the result
                        max_length = cur.fetchone()[0]
                        # Update max_lengths dictionary
                        max_lengths[column_name] = max_length
                        # Construct the ALTER TABLE query
                        alter_query = f"ALTER TABLE {table_name} ALTER COLUMN {column_name} TYPE VARCHAR({max_length});"
                    else:
                        alter_query = f"ALTER TABLE {table_name} ALTER COLUMN {column_name} TYPE {data_type} USING {column_name}::{data_type};"
                    try:
                        # Execute the ALTER TABLE query
                        cur.execute(alter_query)
                    except psycopg2.Error as error:
                        conn.rollback()
                        print(f"Error updating column {column_name} in {table_name}: {error}")
        # Commit the connection
        conn.commit()

    def __add_primary_key(self, table_name, primary_key) -> None:
        """
        Adds a primary key constraint to a PostgreSQL table.

        Parameters:
        - table_name (str): The name of the PostgreSQL table.
        - primary_key (str): The column name or a comma-separated list of column names for the primary key.

        Returns:
        None
        """
        # Connect to the PostgreSQL database
        with psycopg2.connect(self.db_url) as conn:
            with conn.cursor() as cur:
                # Construct the query
                alter_query = f"ALTER TABLE {table_name} ADD PRIMARY KEY ({primary_key});"
                try:
                    # Execute the ALTER TABLE query
                    cur.execute(alter_query)
                except psycopg2.Error as error:
                    conn.rollback()
                    print(f"Error adding primary key to {table_name}: {error}")
        # Commit the connection
        conn.commit()

    def __add_foreign_key(self, table_name, foreign_keys) -> None:
        """
        Adds foreign key constraints to a PostgreSQL table based on the provided dictionary of foreign keys.

        Parameters:
        - table_name (str): The name of the PostgreSQL table.
        - foreign_keys (dict): A dictionary where keys are reference table names and values are foreign key column names.
        """
        # Connect to the PostgreSQL database
        with psycopg2.connect(self.db_url) as conn:
            with conn.cursor() as cur:
                for reference_table, foreign_key in foreign_keys.items():
                    # Construct the query to find the maximum length
                    alter_query = f"ALTER TABLE {table_name} ADD FOREIGN KEY ({foreign_key}) REFERENCES {reference_table}({foreign_key});"
                    try:
                        # Execute the ALTER TABLE query
                        cur.execute(alter_query)
                    except psycopg2.Error as error:
                        conn.rollback()
                        print(f"Error adding foreign key to {table_name}: {error}")
        # Commit the connection
        conn.commit()

    def upload_and_configure_table(
        self,
        data_frame: pd.DataFrame,
        table_name: str,
        column_types: Dict[str, str],
        primary_key: Optional[str] = None,
        foreign_keys: Optional[Dict[str, str]] = None
        ) -> None:
        """
        Uploads a DataFrame to a specified table and configures the table with given column types,
        primary key, and foreign keys.

        Parameters:
            data_frame (pd.DataFrame): DataFrame to be uploaded to the database.
            table_name (str): Name of the table to upload the data to.
            column_types (Dict[str, str]): Dictionary specifying the column types for the table.
            primary_key (Optional[str]): Name of the primary key column (if any).
            foreign_keys (Optional[Dict[str, str]]): Dictionary specifying foreign keys and their corresponding referenced columns.

        Returns:
            None
        """
        try:
            self.__upload_to_db(data_frame, table_name)
            self.__cast_data_types(table_name, column_types)
            
            if primary_key:
                self.__add_primary_key(table_name, primary_key)

            if foreign_keys:
                self.__add_foreign_key(table_name, foreign_keys)
            
            print(f"Data uploaded and configured for '{table_name}' table.")
        except Exception as e:
            print(f"Error uploading and configuring table '{table_name}': {e}")

    @staticmethod
    def store_column_types() -> Dict[str, str]:
        """
        Returns column types for a store table.

        Returns:
            Dict[str, str]: Dictionary specifying the column types for the store table.
        """
        try:
            return {
                'store_type'   : 'VARCHAR(255)',
                'locality'     : 'VARCHAR(255)',
                'continent'    : 'VARCHAR(255)',
                'country_code' : 'VARCHAR(?)',
                'store_code'   : 'VARCHAR(?)',
                'staff_numbers': 'SMALLINT',
                'opening_date' : 'DATE',
                'longitude'    : 'FLOAT',
                'latitude'     : 'FLOAT',
            }
        except Exception as error:
            raise Exception(f"Error defining schema: {str(error)}")

    @staticmethod
    def card_column_types() -> Dict[str, str]:
        """
        Returns column types for a card table.

        Returns:
            Dict[str, str]: Dictionary specifying the column types for the card table.
        """
        try:
            return {
                'card_number'           : 'VARCHAR(?)',
                'expiry_date'           : 'VARCHAR(?)',
                'date_payment_confirmed': 'DATE',
            }
        except Exception as error:
            raise Exception(f"Error defining schema: {str(error)}")

    @staticmethod
    def products_column_types() -> Dict[str, str]:
        """
        Returns column types for a products table.

        Returns:
            Dict[str, str]: Dictionary specifying the column types for the products table.
        """
        try:
            return {
                'product_price_£': 'FLOAT',
                'weight_kg'      : 'FLOAT',
                'int_article_no' : 'VARCHAR(?)',
                'product_code'   : 'VARCHAR(?)',
                'weight_class'   : 'VARCHAR(?)',
                'date_added'     : 'DATE',
                'uuid'           : 'UUID',
                'still_available': 'BOOL',
            }
        except Exception as error:
            raise Exception(f"Error defining schema: {str(error)}")

    @staticmethod
    def date_column_types() -> Dict[str, str]:
        """
        Returns column types for a date table.

        Returns:
            Dict[str, str]: Dictionary specifying the column types for the date table.
        """
        try:
            return {
                'month'      : 'VARCHAR(?)',
                'year'       : 'VARCHAR(?)',
                'day'        : 'VARCHAR(?)',
                'time_period': 'VARCHAR(?)',
                'date_uuid'  : 'UUID',
            }
        except Exception as error:
            raise Exception(f"Error defining schema: {str(error)}")

    @staticmethod
    def user_column_types() -> Dict[str, str]:
        """
        Returns column types for a user table.

        Returns:
            Dict[str, str]: Dictionary specifying the column types for the user table.
        """
        try:
            return {
                'first_name'   : 'VARCHAR(255)',
                'last_name'    : 'VARCHAR(255)',
                'country_code' : 'VARCHAR(?)',
                'user_uuid'    : 'UUID' ,
                'join_date'    : 'DATE',
                'date_of_birth': 'DATE',
            }
        except Exception as error:
            raise Exception(f"Error defining schema: {str(error)}")

    @staticmethod
    def orders_column_types() -> Dict[str, str]:
        """
        Returns column types for a orders table.

        Returns:
            Dict[str, str]: Dictionary specifying the column types for the orders table.
        """
        try:
            return {
                'date_uuid'       : 'UUID',
                'user_uuid'       : 'UUID',
                'card_number'     : 'VARCHAR(?)',
                'store_code'      : 'VARCHAR(?)',
                'product_code'    : 'VARCHAR(?)',
                'product_quantity': 'SMALLINT',
            }
        except Exception as error:
            raise Exception(f"Error defining schema: {str(error)}")

    @staticmethod
    def orders_foreign_keys() -> Dict[str, str]:
        """
        Returns foreign keys for orders table.

        Returns:
            Dict[str, str]: Dictionary specifying foreign keys for the orders table.
        """
        try:
            return {
                'dim_date_times'   : 'date_uuid',
                'dim_users'        : 'user_uuid',
                'dim_card_details' : 'card_number',
                'dim_store_details': 'store_code',
                'dim_products'     : 'product_code',
            }
        except Exception as error:
            raise Exception(f"Error defining schema: {str(error)}")