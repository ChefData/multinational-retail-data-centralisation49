import pandas as pd


class DataCleaning:
    """
    A class for cleaning and preprocessing user data.

    Attributes:
    - None

    Methods:
    - clean_user_data(user_data_df): Cleans the provided user data DataFrame.

    Usage:
    data_cleaner = DataCleaning()
    cleaned_data = data_cleaner.clean_user_data(user_data)
    """

    def clean_user_data(self, user_data_df):
        """
        Cleans the provided user data DataFrame.

        Parameters:
        - user_data_df (pd.DataFrame): The input DataFrame containing user data.

        Returns:
        pd.DataFrame: A cleaned DataFrame with duplicates removed, date errors handled,
                      NULL values dropped, and country code corrections applied.
        """
        # Reset index
        user_data_df = user_data_df.set_index('index')
        
        # Drop duplicate values
        user_data_df = user_data_df.drop_duplicates(subset=None, keep='first', ignore_index=False)

        # Handling errors with dates
        user_data_df['date_of_birth'] = pd.to_datetime(user_data_df['date_of_birth'], format='mixed', errors='coerce')
        user_data_df['join_date'] = pd.to_datetime(user_data_df['join_date'], format='mixed', errors='coerce')
        
        # Handling NULL values
        user_data_df = user_data_df.dropna()

        # Handling incorrectly typed values
        user_data_df['country_code'] = user_data_df['country_code'].str.replace('GGB', 'GB')

        return user_data_df

        '''     
        # Handling incorrectly typed values
        user_data_df['numeric_column'] = pd.to_numeric(user_data_df['numeric_column'], errors='coerce')

        # Handling incorrectly typed values
        make_string = ['first_name', 'last_name', 'company', 'email_address', 'address', 'country', 'country_code']
        user_data_df[make_string] = user_data_df[make_string].astype('string')
          
        # Handling rows filled with the wrong information (you may need to customize this based on your data)
        user_data_df = user_data_df[user_data_df['column_condition'] == 'desired_condition']

        # Remove rows with incorrectly typed values (e.g., non-numeric in a numeric column)
        user_data_df = user_data_df.drop(752)

        # Remove rows with incorrectly typed values (e.g., non-numeric in a numeric column)
        user_data_df = user_data_df.dropna(subset=['numeric_column'])

        # Replace missing values with None
        #user_data_df.fillna(value=0)

        # Drop duplicate values
        #user_data_df.drop_duplicates(subset=None, keep='first', inplace=False, ignore_index=False)

        # Check phone numbers against regular expression
        regex_expression = '^(?:(?:\(?(?:0(?:0|11)\)?[\s-]?\(?|\+)44\)?[\s-]?(?:\(?0\)?[\s-]?)?)|(?:\(?0))(?:(?:\d{5}\)?[\s-]?\d{4,5})|(?:\d{4}\)?[\s-]?(?:\d{5}|\d{3}[\s-]?\d{3}))|(?:\d{3}\)?[\s-]?\d{3}[\s-]?\d{3,4})|(?:\d{2}\)?[\s-]?\d{4}[\s-]?\d{4}))(?:[\s-]?(?:x|ext\.?|\#)\d{3,4})?$'
        user_data_df.loc[~user_data_df['phone_number'].str.match(regex_expression), 'phone_number'] = np.nan  
        user_data_df['phone_number'] = user_data_df['phone_number'].replace({r'\+44':  '0',  r'\(':  '',  r'\)':  '',  r'-':  '',  r'  ':  ''},  regex=True)
        '''

'''
from data_extraction import DataExtractor
from database_utils import DatabaseConnector

if __name__ == "__main__":
    # Creating Class instances
    data_cleaner = DataCleaning()
    data_extractor = DataExtractor()
    db_connector = DatabaseConnector()

    # Using list_db_tables to get the name of the table containing user data
    user_data_table = db_connector.list_db_tables()[1]
    
    # Using read_rds_table to extract the table containing user data
    user_data = data_extractor.read_rds_table(db_connector, user_data_table)

    # Cleaning user data
    cleaned_data = data_cleaner.clean_user_data(user_data)

    # Printing the original DataFrame
    print("User Data:")
    print(user_data_df)
    print(user_data_df.info())

    # Printing the cleaned DataFrame
    print("Cleaned User Data:")
    print(cleaned_data)
    print(cleaned_data.info())
'''