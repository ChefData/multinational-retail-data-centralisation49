import numpy  as np
import pandas as pd


class DataCleaning:
    """
    A class containing static methods for cleaning and preprocessing various types of data.

    Public Methods:
    - clean_user_data(user_data_df: pd.DataFrame) -> pd.DataFrame:
        Clean and preprocess user data DataFrame.

    - clean_card_data(card_data_df: pd.DataFrame) -> pd.DataFrame:
        Clean and preprocess card data DataFrame.

    - called_clean_store_data(store_data_df: pd.DataFrame) -> pd.DataFrame:
        Clean and preprocess store data DataFrame.

    - convert_product_weights(product_data_df: pd.DataFrame) -> pd.DataFrame:
        Convert product weights to a standardised unit (kilograms).

    - clean_products_data(product_data_df: pd.DataFrame) -> pd.DataFrame:
        Clean and preprocess product data DataFrame.

    - clean_orders_data(orders_data_df: pd.DataFrame) -> pd.DataFrame:
        Clean and preprocess orders data DataFrame.

    - clean_date_data(date_data_df: pd.DataFrame) -> pd.DataFrame:
        Clean and preprocess date data DataFrame.
    """

    @staticmethod
    def clean_user_data(user_data_df) -> pd.DataFrame:
        """
        Clean and preprocess user data DataFrame.

        Parameters:
        - user_data_df (pd.DataFrame): Input DataFrame containing user data.

        Returns:
        - pd.DataFrame: Cleaned user data DataFrame.
        """
        # Handling incorrectly typed values
        user_data_df = user_data_df.replace({'country_code': {'GGB': 'GB'}, 'email_address': {'@@': '@', 'ä': 'a'}}, regex=True)

        # Handling errors with dates and dropping NULL values
        date_columns = ['date_of_birth', 'join_date']
        user_data_df[date_columns] = user_data_df[date_columns].apply(pd.to_datetime, format='mixed', errors='coerce')
        user_data_df.dropna(subset=date_columns, inplace=True)
        
        # Check email address against regular expression
        email_regex_expression = r"^[a-z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[a-z0-9!#$%&'*+/=?^_`{|}~-]+)*@(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\.)+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?$" 
        user_data_df['email_address_check'] = user_data_df['email_address'].str.match(email_regex_expression)

        # Remove phone extension from phone_number and create new column
        user_data_df['phone_number'] = user_data_df['phone_number'].str.replace(" ","")
        user_data_df[['phone_number', 'phone_extension']] = user_data_df['phone_number'].str.split('x', n=1, expand=True)

        # Handling incorrectly typed phone numbers with regular expression patterns 
        country_patterns = {
            'GB': {r'\+44\(0\)': '0', r'\+44': '0', r'\(': '',r'\)': ''},
            'DE': {r'\+49\(0\)': '0'},
            'US': {r'\+1': '', r'001\-': '', r'\(': '', r'\)': '', r'\-': '', r'\.': ''},
        }
        for country, pattern_dict in country_patterns.items():
            mask = user_data_df['country_code'] == country
            user_data_df.loc[mask, 'phone_number'] = user_data_df.loc[mask, 'phone_number'].replace(pattern_dict, regex=True)
        
        # Check phone numbers against country specific regular expression
        country_regex = {
            'GB': "^(?:(?:\(?(?:0(?:0|11)\)?[\s-]?\(?|\+)44\)?[\s-]?(?:\(?0\)?[\s-]?)?)|(?:\(?0))(?:(?:\d{5}\)?[\s-]?\d{4,5})|(?:\d{4}\)?[\s-]?(?:\d{5}|\d{3}[\s-]?\d{3}))|(?:\d{3}\)?[\s-]?\d{3}[\s-]?\d{3,4})|(?:\d{2}\)?[\s-]?\d{4}[\s-]?\d{4}))(?:[\s-]?(?:x|ext\.?|\#)\d{3,4})?$",
            'DE': "(\(?([\d \-\)\-\+\/\(]+){6,}\)?([ .\--\/]?)([\d]+))",
            'US': "^(?:\+?1[-\.\s]?)?\(?([2-9][0-8][0-9])\)?[-\.\s]?([2-9][0-9]{2})[-\.\s]?([0-9]{4})$",
        }
        for country, phone_regex_expression in country_regex.items():
            mask = user_data_df['country_code'] == country
            user_data_df.loc[mask, 'phone_number_check'] = user_data_df.loc[mask, 'phone_number'].str.match(phone_regex_expression)

        # Format the phone numbers with parentheses and hyphen if they have exactly 10 digits and country code is 'US'
        mask = (user_data_df['country_code'] == 'US') & user_data_df['phone_number'].str.match('^\d{10}$')
        user_data_df.loc[mask, 'phone_number'] = user_data_df.loc[mask, 'phone_number'].str.replace('(\d{3})(\d{3})(\d{4})', r'(\1) \2-\3', regex=True)

        # Convert data types      
        user_data_df = user_data_df.astype({
            'first_name': 'string',
            'last_name': 'string',
            'email_address': 'string',
            'address': 'string',
            'phone_number': 'string',
            'phone_extension': 'string',
            'user_uuid': 'string',
            'company': 'category',
            'country': 'category',
            'country_code': 'category',
            'phone_number_check': 'bool'
        })

        # Reorder columns
        user_data_df = user_data_df[['first_name', 'last_name', 'date_of_birth', 'company', 'address', 'country', 'country_code', 'user_uuid', 'join_date', 'email_address', 'email_address_check', 'phone_number', 'phone_extension', 'phone_number_check']]
        
        # Return cleaned DataFrame
        return user_data_df
    
    @staticmethod    
    def clean_card_data(card_data_df) -> pd.DataFrame:
        """
        Clean and preprocess card data DataFrame.

        Parameters:
        - card_data_df (pd.DataFrame): Input DataFrame containing card data.

        Returns:
        - pd.DataFrame: Cleaned card data DataFrame.
        """
        # Handling NULL values
        card_data_df = card_data_df.dropna()

        # Drop card numbers containing letters
        card_data_df = card_data_df[~card_data_df['card_number'].astype(str).str.contains('[a-zA-Z]')]

        # Handling incorrectly typed values
        card_data_df['card_number'] = card_data_df['card_number'].replace({'\?': ''}, regex=True)

        # Handling errors with dates
        date_formats = {'expiry_date': '%m/%y', 'date_payment_confirmed': 'mixed'}
        for column, date_format in date_formats.items():
            card_data_df[column] = pd.to_datetime(card_data_df[column], format=date_format, errors='coerce')

        # Convert data types
        card_data_df['card_number'] = card_data_df['card_number'].astype('string')
        card_data_df['card_provider'] = card_data_df['card_provider'].astype('category')
        
        # Return cleaned DataFrame
        return card_data_df
    
    @staticmethod    
    def called_clean_store_data(store_data_df) -> pd.DataFrame:
        """
        Clean and preprocess store data DataFrame.

        Parameters:
        - store_data_df (pd.DataFrame): Input DataFrame containing store data.

        Returns:
        - pd.DataFrame: Cleaned store data DataFrame.
        """
        # Reset index
        store_data_df = store_data_df.set_index('index')
        
        # Drop lat column
        #store_data_df["latitude"] = store_data_df["latitude"].combine_first(store_data_df["lat"])
        store_data_df.drop('lat', axis='columns', inplace=True, errors='ignore')

        # Handling errors with dates
        store_data_df['opening_date'] = pd.to_datetime(store_data_df['opening_date'], format='mixed', errors='coerce')
        store_data_df.dropna(subset='opening_date', inplace=True)

        # Handling incorrectly typed values
        store_data_df['continent'] = store_data_df['continent'].str.replace('ee', '')
        store_data_df['staff_numbers'] = store_data_df['staff_numbers'].str.replace(r'\D', '', regex=True)

        # Replace 'N/A' with NaN in specified columns
        columns_to_replace_na = ['latitude', 'longitude']
        store_data_df[columns_to_replace_na] = store_data_df[columns_to_replace_na].replace('N/A', np.nan)

        # Rename incorrectly labeled columns
        store_data_df.rename(columns = {'latitude': 'longitude', 'longitude': 'latitude'}, inplace = True)

        # Descriptive names for regex expressions
        lat_regex = '^(\+|-)?(?:90(?:(?:\.0{1,6})?)|(?:[0-9]|[1-8][0-9])(?:(?:\.[0-9]{1,6})?))$'
        lon_regex = '^(\+|-)?(?:180(?:(?:\.0{1,6})?)|(?:[0-9]|[1-9][0-9]|1[0-7][0-9])(?:(?:\.[0-9]{1,6})?))$'

        # Check columns against regular expression and use assign for column creation
        store_data_df = store_data_df.assign(
            latitude_check  = store_data_df['latitude'] .str.match(lat_regex),
            longitude_check = store_data_df['longitude'].str.match(lon_regex),
        )

        # Convert data types
        store_data_df = store_data_df.astype({
            'store_code': 'string',
            'address': 'string',
            'locality': 'string',
            'store_type': 'category',
            'country_code': 'category',
            'continent': 'category',
            'latitude': 'float',
            'longitude': 'float',
            'staff_numbers': 'int'
        })

        # Reorder columns
        store_data_df = store_data_df[['store_code', 'store_type', 'opening_date', 'staff_numbers', 'address', 'locality', 'country_code', 'continent', 'latitude', 'longitude', 'latitude_check', 'longitude_check']]

        # Return cleaned DataFrame
        return store_data_df
    
    @staticmethod    
    def convert_product_weights(product_data_df) -> pd.DataFrame:
        """
        Convert product weights to a standardised unit (kilograms).

        Parameters:
        - product_data_df (pd.DataFrame): Input DataFrame containing product data.

        Returns:
        - pd.DataFrame: DataFrame with weights converted to kilograms.
        """
        # Extract numeric values and units
        weight_components = product_data_df['weight'].str.extract(r'(\d+\.\d*|\d*\.\d+|\d+)\s*(?:x\s*(\d+\.\d*|\d*\.\d+|\d+))?\s*([a-zA-Z]+)')

        # Convert the numeric values to numeric type
        weight_components[[0, 1]] = weight_components[[0, 1]].apply(pd.to_numeric, errors='coerce')

        # Multiply the two columns to get the final weight
        weight_components['weight'] = np.where(weight_components[1].notna(), weight_components[0] * weight_components[1], weight_components[0])

        # Create a dictionary for unit conversions
        unit_conversion = {'g': 1000, 'ml': 1000, 'oz': 35.27396195}
        
        # Apply unit conversion to the 'weight' column
        for units, conversion in unit_conversion.items():
            weight_components.loc[weight_components[2] == units, 'weight'] /= conversion

        # Drop unnecessary columns
        product_data_df['weight'] = weight_components['weight']
        product_data_df.rename(columns={'weight': 'weight_kg'}, inplace=True)

        # Return cleaned DataFrame
        return product_data_df

    @staticmethod    
    def clean_products_data(product_data_df) -> pd.DataFrame:
        """
        Clean and preprocess product data DataFrame.

        Parameters:
        - product_data_df (pd.DataFrame): Input DataFrame containing product data.

        Returns:
        - pd.DataFrame: Cleaned product data DataFrame.
        """
        # Reset index
        product_data_df = product_data_df.set_index('Unnamed: 0')

        # Chained operations for handling NULL values and incorrect rows
        product_data_df = (
            product_data_df
            .dropna()
            .loc[~product_data_df['product_price'].astype(str).str.contains('[a-zA-Z]')]
        )
        
        # Descriptive names for regex expressions
        ean_regex = '^(?:\d{8}|\d{12}|\d{13}|\d{14})$'
        pc_regex = '[a-zA-Z]\d-[0-9]+[a-zA-Z]?'
        uuid_regex = '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'

        # Check columns against regular expression and use assign for column creation
        product_data_df = product_data_df.assign(
            int_article_no_check = product_data_df['EAN']         .str.match(ean_regex),
            product_code_check   = product_data_df['product_code'].str.match(pc_regex),
            uuid_check           = product_data_df['uuid']        .str.match(uuid_regex),
        )

        # Adds a new column weight_class which will contain human-readable values based on the weight range of the product.
        bins = [0, 2, 40, 140, 10000]
        labels = ['Light', 'Mid_Sized', 'Heavy', 'Truck_Required']
        product_data_df['weight_class'] = pd.cut(product_data_df['weight_kg'], bins=bins, right=False, labels=labels)

        # Handling errors with dates
        product_data_df['date_added'] = pd.to_datetime(product_data_df['date_added'], format='mixed', errors='coerce')
        
        # Consistent price handling using str.replace
        product_data_df['product_price'] = product_data_df['product_price'].str.replace('£', '').apply(pd.to_numeric, errors='coerce')
        
        # Convert removed column into bool
        product_data_df = product_data_df.replace({'removed': {'Still_avaliable': True, 'Removed': False}}, regex=True)

        # Rename columns
        product_data_df.rename(columns={'product_price': 'product_price_£', 'EAN': 'int_article_no', 'removed': 'still_available'}, inplace=True)

        # Convert data types
        product_data_df = product_data_df.astype({
            'product_name': 'string',
            'int_article_no': 'string',
            'uuid': 'string',
            'product_code': 'string',
            'category': 'category',
            'still_available': 'bool',
        })

        # Return cleaned DataFrame
        return product_data_df

    @staticmethod    
    def clean_orders_data(orders_data_df) -> pd.DataFrame:
        """
        Clean and preprocess orders data DataFrame.

        Parameters:
        - orders_data_df (pd.DataFrame): Input DataFrame containing orders data.

        Returns:
        - pd.DataFrame: Cleaned orders data DataFrame.
        """
        # Reset index
        orders_data_df.set_index('level_0', inplace=True)

        # Drop columns
        orders_data_df.drop(['first_name', 'last_name', '1'], axis='columns', inplace=True)

        # Return cleaned DataFrame
        return orders_data_df

    @staticmethod    
    def clean_date_data(date_data_df) -> pd.DataFrame:
        """
        Clean and preprocess date data DataFrame.

        Parameters:
        - date_data_df (pd.DataFrame): Input DataFrame containing date data.

        Returns:
        - pd.DataFrame: Cleaned date data DataFrame.
        """
        # Handling incorrectly entered rows
        date_data_df = date_data_df[~date_data_df['month'].astype(str).str.contains('[a-zA-Z]')]

        # Handling errors with dates
        date_data_df['date_time'] = pd.to_datetime(date_data_df[['year', 'month', 'day']].astype(str).agg('-'.join, axis=1) + ' ' + date_data_df['timestamp'], errors='coerce')

        # Convert data types
        date_data_df[['date_uuid', 'time_period']] = date_data_df[['date_uuid', 'time_period']].astype('string')

        # Drop columns
        #date_data_df.drop(['month', 'year', 'day', 'timestamp'], axis='columns', inplace=True)

        # Reorder columns
        date_data_df = date_data_df[['date_uuid', 'date_time', 'time_period', 'month', 'year', 'day', 'timestamp']]

        # Return cleaned DataFrame
        return date_data_df