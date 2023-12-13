import pandas as pd
import numpy  as np
import re

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
        # Drop duplicate values
        user_data_df = user_data_df.drop_duplicates(subset=None, keep='first', ignore_index=False)
        
        # Handling errors with dates
        user_data_df['date_of_birth'] = pd.to_datetime(user_data_df['date_of_birth'], format='mixed', errors='coerce')
        user_data_df['join_date'] = pd.to_datetime(user_data_df['join_date'], format='mixed', errors='coerce')

        # Handling NULL values
        user_data_df = user_data_df.dropna()

        # Handling incorrectly typed values
        user_data_df['country_code'] = user_data_df['country_code'].str.replace('GGB', 'GB')
        user_data_df['email_address'] = user_data_df['email_address'].replace({r'@@':'@', r'ä':'a'}, regex=True)
        
        us_mask = (user_data_df['country_code'] == 'US')
        de_mask = (user_data_df['country_code'] == 'DE')
        gb_mask = (user_data_df['country_code'] == 'GB')
        
        user_data_df['phone_number'] = user_data_df['phone_number'].str.replace(" ","")
        user_data_df['phone_extension'] = user_data_df['phone_number'].str.split('x', n=1).str[1]
        user_data_df['phone_number'] = user_data_df['phone_number'].str.split('x', n=1).str[0]

        user_data_df.loc[gb_mask, 'phone_number'] = user_data_df.loc[gb_mask, 'phone_number'].replace({
            r"\+44\(0\)": "0",
            r"\+44": "0",
            r"\(": "",
            r"\)": ""
        }, regex=True)

        user_data_df.loc[de_mask, 'phone_number'] = user_data_df.loc[de_mask, 'phone_number'].replace({
            r"\+49\(0\)": "0"
        }, regex=True)
        
        user_data_df.loc[us_mask, 'phone_number'] = user_data_df.loc[us_mask, 'phone_number'].replace({
            r"\+1": "",
            r"001\-": "",
            r"\(": "",
            r"\)": "",
            r"\-": "",
            r"\.": ""
        }, regex=True)

        # Check email address against regular expression
        email_regex_expression = r"^[a-z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[a-z0-9!#$%&'*+/=?^_`{|}~-]+)*@(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\.)+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?$" 
        user_data_df['email_address_check'] = user_data_df['email_address'].map(lambda i: bool(re.match(email_regex_expression, i)))

        # Check phone numbers against international regular expression
        #phone_regex_expression = '^(?:(?:\(?(?:0(?:0|11)\)?[\s-]?\(?|\+)44\)?[\s-]?(?:\(?0\)?[\s-]?)?)|(?:\(?0))(?:(?:\d{5}\)?[\s-]?\d{4,5})|(?:\d{4}\)?[\s-]?(?:\d{5}|\d{3}[\s-]?\d{3}))|(?:\d{3}\)?[\s-]?\d{3}[\s-]?\d{3,4})|(?:\d{2}\)?[\s-]?\d{4}[\s-]?\d{4}))(?:[\s-]?(?:x|ext\.?|\#)\d{3,4})?$'
        #user_data_df['phone_number_check'] = user_data_df['phone_number'].map(lambda i: bool(re.match(phone_regex_expression, i)))
        
        # Check German phone numbers against country specific regular expression
        de_phone_regex_expression = '(\(?([\d \-\)\-\+\/\(]+){6,}\)?([ .\--\/]?)([\d]+))'
        user_data_df.loc[de_mask, 'phone_number_check'] = user_data_df.loc[de_mask, 'phone_number'].map(lambda i: bool(re.match(de_phone_regex_expression, i)))
        
        # Check American phone numbers against country specific regular expression
        us_phone_regex_expression = '^(?:\+?1[-\.\s]?)?\(?([2-9][0-8][0-9])\)?[-\.\s]?([2-9][0-9]{2})[-\.\s]?([0-9]{4})$'
        user_data_df.loc[us_mask, 'phone_number_check'] = user_data_df.loc[us_mask, 'phone_number'].map(lambda i: bool(re.match(us_phone_regex_expression, i)))
        
        # Check British phone numbers against country specific regular expression
        gb_phone_regex_expression = '^(?:(?:\(?(?:0(?:0|11)\)?[\s-]?\(?|\+)44\)?[\s-]?(?:\(?0\)?[\s-]?)?)|(?:\(?0))(?:(?:\d{5}\)?[\s-]?\d{4,5})|(?:\d{4}\)?[\s-]?(?:\d{5}|\d{3}[\s-]?\d{3}))|(?:\d{3}\)?[\s-]?\d{3}[\s-]?\d{3,4})|(?:\d{2}\)?[\s-]?\d{4}[\s-]?\d{4}))(?:[\s-]?(?:x|ext\.?|\#)\d{3,4})?$'
        #gb_phone_regex_expression = '^((((\(?0\d{4}\)?\s?\d{3}\s?\d{3})|(\(?0\d{3}\)?\s?\d{3}\s?\d{4})|(\(?0\d{2}\)?\s?\d{4}\s?\d{4}))(\s?\(\d{4}|\d{3}))?)|((\+44\s?7\d{3}|\(?07\d{3}\)?)\s?\d{3}\s?\d{3})|((((\+44\s?\d{4}|\(?0\d{4}\)?)\s?\d{3}\s?\d{3})|((\+44\s?\d{3}|\(?0\d{3}\)?)\s?\d{3}\s?\d{4})|((\+44\s?\d{2}|\(?0\d{2}\)?)\s?\d{4}\s?\d{4}))(\s?\(\d{4}|\d{3}))?$'
        user_data_df.loc[gb_mask, 'phone_number_check'] = user_data_df.loc[gb_mask, 'phone_number'].map(lambda i: bool(re.match(gb_phone_regex_expression, i)))

        # Check if the phone number has exactly 10 digits and country code is 'US'
        mask = (user_data_df['country_code'] == 'US') & user_data_df['phone_number'].str.match('^\d{10}$')
        # Format the phone numbers with parentheses and hyphen if they have exactly 10 digits and country code is 'US'
        user_data_df.loc[mask, 'phone_number'] = user_data_df.loc[mask, 'phone_number'].str.replace('(\d{3})(\d{3})(\d{4})', r'(\1) \2-\3', regex=True)

        # Convert data types
        make_string = ['first_name', 'last_name', 'email_address', 'address', 'phone_number', 'phone_extension', 'user_uuid']
        make_category = ['company', 'country', 'country_code']
        user_data_df[make_string] = user_data_df[make_string].astype('string')
        user_data_df[make_category] = user_data_df[make_category].astype('category')
        user_data_df['phone_number_check'] = user_data_df['phone_number_check'].astype('bool')

        # Reorder columns
        reorder_columns = ['first_name', 'last_name', 'date_of_birth', 'company', 'address', 'country', 'country_code', 'user_uuid', 'join_date', 'email_address', 'email_address_check', 'phone_number', 'phone_extension', 'phone_number_check']
        user_data_df = user_data_df.reindex(columns=reorder_columns)

        # Return cleaned DataFrame
        return user_data_df
    
    def clean_card_data(self, card_data_df):
        # Handling NULL values
        card_data_df = card_data_df.dropna()

        # Drop card numbers containing letters
        mask = card_data_df['card_number'].apply(lambda x: pd.notna(x) and not any(c.isalpha() for c in str(x)))
        card_data_df = card_data_df[mask]
        
        # Handling incorrectly typed values
        card_data_df['card_number'] = card_data_df['card_number'].replace({'\?': ''}, regex=True)

        # Drop duplicate values
        card_data_df = card_data_df.drop_duplicates(subset=None, keep='first', ignore_index=False)
        
        # Handling errors with dates
        card_data_df['expiry_date'] = pd.to_datetime(card_data_df['expiry_date'], format="%m/%y", errors='coerce')
        card_data_df['date_payment_confirmed'] = pd.to_datetime(card_data_df['date_payment_confirmed'], format="%Y-%m-%d", errors='coerce')
        
        # Convert data types
        card_data_df['card_number'] = card_data_df['card_number'].astype('string')
        card_data_df['card_provider'] = card_data_df['card_provider'].astype('category')

        # Return cleaned DataFrame
        return card_data_df
    
    def called_clean_store_data(self, store_data_df):
        # Reset index
        store_data_df = store_data_df.set_index('index')
        
        # Handling incorrectly entered rows
        mask = store_data_df['longitude'].apply(lambda x: pd.notna(x) and not any(c.isalpha() for c in str(x)))
        store_data_df = store_data_df[mask]
        
        # Drop lat column
        store_data_df.drop("lat", axis="columns", inplace=True)
        
        # Handling incorrectly typed values
        store_data_df['continent'] = store_data_df['continent'].str.replace('ee', '')
        
        # Remove letters from int column
        store_data_df['staff_numbers'] = store_data_df['staff_numbers'].apply(lambda x: ''.join(char for char in x if char.isdigit()))

        # Handling errors with dates
        store_data_df['opening_date'] = pd.to_datetime(store_data_df['opening_date'], format='mixed', errors='coerce')

        # Rename incorrectly labeled columns
        store_data_df.rename(columns = {'latitude':'longitude', 'longitude':'latitude'}, inplace = True)

        # Check latitude against regular expression
        lat_regex_expression = '^(\+|-)?(?:90(?:(?:\.0{1,6})?)|(?:[0-9]|[1-8][0-9])(?:(?:\.[0-9]{1,6})?))$'
        store_data_df['latitude_check'] = store_data_df['latitude'].map(lambda i: bool(re.match(lat_regex_expression, i)))
        
        # Check longitude against regular expression
        lon_regex_expression = '^(\+|-)?(?:180(?:(?:\.0{1,6})?)|(?:[0-9]|[1-9][0-9]|1[0-7][0-9])(?:(?:\.[0-9]{1,6})?))$'
        store_data_df['longitude_check'] = store_data_df['longitude'].map(lambda i: bool(re.match(lon_regex_expression, i)))

        # Reorder columns
        reorder_columns = ['store_code', 'store_type', 'opening_date', 'staff_numbers', 'address', 'locality', 'country_code', 'continent', 'latitude', 'longitude', 'latitude_check', 'longitude_check']
        store_data_df = store_data_df.reindex(columns=reorder_columns)

        # Convert data types
        make_string = ['store_code', 'address', 'locality']
        make_category = ['store_type', 'country_code', 'continent']
        make_interger = ['latitude', 'longitude']
        store_data_df[make_string] = store_data_df[make_string].astype('string')
        store_data_df[make_category] = store_data_df[make_category].astype('category')
        store_data_df[make_interger] = store_data_df[make_interger].astype('int')

        # Return cleaned DataFrame
        return store_data_df
    
    def convert_product_weights(self, product_data_df):
        # Extract numeric values and units
        weight_components = product_data_df['weight'].str.extract(r'(\d+\.\d*|\d*\.\d+|\d+)\s*(?:x\s*(\d+\.\d*|\d*\.\d+|\d+))?\s*([a-zA-Z]+)')

        # Convert the numeric values to numeric type
        weight_components[[0, 1]] = weight_components[[0, 1]].apply(pd.to_numeric, errors='coerce')
        
        # Multiply the two columns to get the final weight
        weight_components['weight'] = np.where(weight_components[1].notna(), weight_components[0] * weight_components[1], weight_components[0])

        # Convert units to kg
        weight_components.loc[weight_components[2] ==  'g', 'weight'] /= 1000
        weight_components.loc[weight_components[2] == 'ml', 'weight'] /= 1000
        weight_components.loc[weight_components[2] == 'oz', 'weight'] /= 35.27396195

        # Drop unnecessary columns
        product_data_df['weight'] = weight_components['weight'].to_frame()

        return product_data_df

    def clean_products_data(self, product_data_df):
        # Reset index
        product_data_df = product_data_df.set_index('Unnamed: 0')

        # Handling NULL values
        product_data_df = product_data_df.dropna()
        
        # Drop duplicate values
        product_data_df = product_data_df.drop_duplicates(subset=None, keep='first', ignore_index=False)

        # Handling incorrectly entered rows
        mask = product_data_df['product_price'].apply(lambda x: pd.notna(x) and not any(c.isalpha() for c in str(x)))
        product_data_df = product_data_df[mask]

        # Check international article number against  regular expression
        ean_regex_expression = '^(?:\d{8}|\d{12}|\d{13}|\d{14})$'
        product_data_df['int_article_no_check'] = product_data_df['EAN'].map(lambda i: bool(re.match(ean_regex_expression, i)))

        # Check product code against regular expression
        pc_regex_expression = '[a-zA-Z]\d-[0-9]+[a-zA-Z]?'
        product_data_df['product_code_check'] = product_data_df['product_code'].map(lambda i: bool(re.match(pc_regex_expression, i)))

        # Check Universally Unique Identifier against regular expression
        uuid_regex_expression = '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
        product_data_df['uuid_check'] = product_data_df['uuid'].map(lambda i: bool(re.match(uuid_regex_expression, i)))

        # Handling errors with dates
        product_data_df['date_added'] = pd.to_datetime(product_data_df['date_added'], format='mixed', errors='coerce')

        # Removing £ sign
        product_data_df['product_price'] = product_data_df['product_price'].replace({'\£': ''}, regex=True)

        # Rename columns
        product_data_df.rename(columns={'product_price': 'product_price_£', 'EAN': 'int_article_no', 'weight': 'weight_kg'}, inplace=True)

        # Convert data types
        make_string = ['product_name', 'int_article_no', 'uuid', 'product_code']
        make_category = ['category', 'removed']
        make_float = ['product_price_£']
        product_data_df[make_string] = product_data_df[make_string].astype('string')
        product_data_df[make_category] = product_data_df[make_category].astype('category')
        product_data_df[make_float] = product_data_df[make_float].astype('float')

        return product_data_df


        '''
        The following are further data cleaning examples that were not used during this process:

        # Reset index
        #user_data_df = user_data_df.set_index('index')
        
        # Handling incorrectly typed values
        user_data_df['numeric_column'] = pd.to_numeric(user_data_df['numeric_column'], errors='coerce')
          
        # Handling rows filled with the wrong information
        user_data_df = user_data_df[user_data_df['column_condition'] == 'desired_condition']

        # Remove rows with incorrectly typed values (e.g., non-numeric in a numeric column)
        user_data_df = user_data_df.drop()

        # Remove rows with incorrectly typed values (e.g., non-numeric in a numeric column)
        user_data_df = user_data_df.dropna(subset=['numeric_column'])

        # Replace missing values with None
        #user_data_df.fillna(value=0)

        # Drop duplicate values
        #user_data_df.drop_duplicates(subset=None, keep='first', inplace=False, ignore_index=False)
        '''
