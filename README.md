# Multinational Retail Data Centralisation

## Table of Contents

- [Description of the project](#description-of-the-project)
  - [What the project does](#what-the-project-does)
  - [Aim of the project](#aim-of-the-project)
  - [Lessons learned](#lessons-learned)
- [Installation instructions](#installation-instructions)
- [Usage instructions](#usage-instructions)
  - [Environment Setup](#environment-setup)
  - [AWS Configuration](#aws-configuration)
  - [Credential Setup](#credential-setup)
  - [Connect to local database](#connect-to-local-database)
  - [Project Navigation](#project-navigation)
- [Classes and Methods](#classes-and-methods)
  - [DatabaseConnector](#databaseconnector)
  - [DataExtractor](#dataextractor)
  - [DataLoader](#dataloader)
  - [DataCleaning](#datacleaning)
- [File structure of the project](#file-structure-of-the-project)
- [Tools Used](#tools-used)
- [Troubleshooting](#troubleshooting)

## Description of the project

A multinational company sells various goods across the globe. Their sales data is spread across many different data sources, making it inaccessible for analysis by current team members. To become more data-driven, the organisation would like to make its sales data accessible from one centralised location.

1. The first goal is to produce a system that stores the current company data in a database so that it's accessed from one centralised location and acts as a single source of truth for sales data.
2. The second goal is to query the database to get up-to-date metrics for the business.

This project includes a set of Python scripts and classes for processing and cleaning various types of data.

Data has been stored in five different sources, including:

- The users' historical data is stored in an AWS RDS database in the cloud.
- The users' card details are stored in a PDF document in an AWS S3 bucket.
- The store data can be retrieved through the use of an API.
- The information for each product the company sells is stored in CSV format in an S3 bucket on AWS.
- This table, which acts as the single source of truth for all orders the company has made in the past, is stored in a database on AWS RDS.
- The final data source is a series of JSON objects containing the details of when each sale happened and related attributes.

### What the project does

In this project, the following actions will take place:

1. How to connect and extract data from multiple sources e.g. a csv file, a PDF, an S3 Bucket, an API, and a json file.
2. How to clean data effectively by considering NULL values, duplicates, valid data types, incorrectly typed values, wrong information, and formatting.
3. How to organise code effectively into suitable classes to connect, extract, and clean data.
4. How to query the data using pgAdmin 4 to cast data types, manipulate the tables, and set primary and foreign keys.
5. How to query the data using pgAdmin 4 to generate insights such as:
   - Which locations have the most stores?
   - Which month produced the largest number of sales?
   - How many sales are coming from online?
   - What percentage of sales come through each type of store?

### Aim of the project

This project has been designed to demonstrate proficiency in techniques related to data handling, including SQL, database management, data manipulation, and data retrieval using APIs.

This project aims to test knowledge of the Python programming language, the SQL programming language, git and GitHub, the command interface, cloud computing, and web APIs.

### Lessons learned

The following is a condensed list of some of the things I put into practice after learning them through the AiCore Bootcamp:

#### General

- Created classes to encapsulate the code
- Defined functions to abstract code
- Set up conda environments to isolate the project dependencies
- Created try except blocks for error handling
- Used descriptive names for methods and variables to enhance code readability.
- Eliminated code duplication by identifying and refactoring repeated code blocks into separate methods or functions.
- Ensured that each method has a single responsibility, focusing on a specific task, based on the Single Responsibility Principle (SRP).
- Wrote access modifiers by making methods private or protected when intended for internal use within the class and not externally accessible.
- Used the `if __name__ == "__main__"` statement to include code blocks that should only run when the script was executed directly, not when imported as a module.
- Wrote with a consistent import order by organising the import statements in a consistent manner: alphabetically, with from statements before import statements to maintain readability.
- Minimised nested loops to improve code efficiency and reduce complexity.
- Imported only the specific methods or classes needed from a module to enhance code clarity and prevent naming conflicts.
- Wrote consistent Docstrings for all methods, explaining their purpose, parameters, return values, and errors raised.
- Added type annotations to method signatures to improve code maintainability and catch type-related errors during development
- Used comprehensions to provide a concise and elegant way to create new sequences, such as lists, sets, and dictionaries, by iterating over existing ones
- Used staticmethods to unbound methods from the class

#### Data Extraction

- Set up and connected to a database engine
- Used context managers to manage resources properly
- Read SQL tables from AWS Relational Database Service
- Read a PDF from a link and stitched the data together into a DataFrame
- Sent a GET request to the API endpoint
- Connected to AWS S3 buckets

#### Data Transformation

- Handled incorrectly typed values using regular expressions
- Handled errors with dates using pd.to_datetime
- Dropped NULL values
- Dropped columns from a DataFrame
- Checked email addresses against a regular expression
- Checked phone numbers against regular expressions
- Split strings into multiple parts
- Converted data types
- Rearranged the columns in a DataFrame
- Used for loops to iterate over a DataFrame with a dictionary
- Reset the index of a DataFrame

#### Data Load

- Connected to a PostgreSQL database
- Queried SQL tables with psycopg2
- Altered data types of SQL tables with psycopg2
- Set primary and foreign keys of SQL tables with psycopg2
- Developed a star-based schema of the database, ensuring that the columns are of the correct data types.

#### Data Querying

- Used PostgreSQL to query the data to understand the company sales better
- Used joins to combine data from across tables
- Used Case statements for "IF this THEN that" scenarios
- Used Common Table Expressions to cleanup complicated queries

## Installation instructions

To use the data processing and cleaning functionality provided by this project, follow these steps:

> [!NOTE]
> Make sure you have the following installed:
>
> - A Code editor such as Visual Studio Code
> - Conda (optional but recommended)
> - pgAdmin4

### 1. Clone the Repository

Clone the repository to your local machine using the following:

#### Windows

1. Install [Git](https://git-scm.com/download/win).
2. Open the command prompt or Git Bash.
3. Clone the repository to your local machine:

    ```bash
    git clone https://github.com/ChefData/multinational-retail-data-centralisation49
    ```

#### macOS

1. Open the Terminal.
2. If you don't have git installed, you can install it using [Homebrew](https://brew.sh/):

    ```bash
    /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
    brew install git
    ```

3. Clone the repository to your local machine:

    ```bash
    git clone https://github.com/ChefData/multinational-retail-data-centralisation49
    ```

#### Linux: Ubuntu or Debian-based systems

1. Open the Terminal.
2. Install git:

    ```bash
    sudo apt-get update
    sudo apt-get install git
    ```

3. Clone the repository to your local machine:

    ```bash
    git clone https://github.com/ChefData/multinational-retail-data-centralisation49
    ```

#### Linux: Fedora

1. Open the Terminal.
2. Install git:

    ```bash
    sudo dnf install git
    ```

3. Clone the repository to your local machine:

    ```bash
    git clone https://github.com/ChefData/multinational-retail-data-centralisation49
    ```

## Usage instructions

> [!NOTE]
> It is assumed that you are a support engineer at AiCore and have the relevant credentials to download the data that has been stored in the different sources

Follow these instructions to set up and install the project on your local machine.

### Environment Setup

1. Create a Conda Virtual Environment to isolate the project dependencies (Optional but Recommended)

    ```bash
    conda create -n AiCore-Project-MRDC python=3.11 ipykernel pandas PyYAML sqlalchemy postgresql tabula-py psycopg2 requests boto3 awscli python-decouple
    ```

2. Or import the conda environment from the supplied YAML file

    ```bash
    conda env create -f AiCore-Project-MRDC-env.yml
    ```

3. Activate the conda virtual environment:

- On Windows:

    ```bash
    activate AiCore-Project-MRDC
    ```

- On macOS and Linux:

    ```bash
    conda activate AiCore-Project-MRDC
    ```

### AWS Configuration

> [!NOTE]
> You must be logged into the AWS CLI before retrieving the data from the S3 bucket.

1. Open a terminal or command prompt on your local machine
2. Run the following command to start the AWS CLI configuration process:

    ```bash
    aws configure
    ```

3. You will be prompted to enter the following information:
    - AWS Access Key ID: Enter the access key ID you obtained during the access key generation process
    - AWS Secret Access Key: Enter the secret access key corresponding to the access key ID you provided
    - Default region name: Specify the default AWS region you want to use for AWS CLI commands. In our case, we will use eu-west-1, as this region is geographically close to the UK.
    - Default output format: Choose the default output format for AWS CLI command results. You can enter JSON, text, table, or YAML. The default format is typically JSON, which provides machine-readable output. If you enter nothing (press Enter) it will default to JSON.

4. After entering the required information, press Enter
5. To verify that the configuration was successful, run the following command:

    ```bash
    aws configure list
    ```

6. This command will display the configuration settings, including the access key ID, secret access key (partially masked), default region, and default output format. Make sure the displayed values match the credentials you provided during the configuration.

### Credential Setup

1. Initialise a new database locally within pgAdmin4 to store the extracted data.
    - Set up the new database and name it sales_data.
    - This database will store all the company information once you extract it from various data sources.

2. Create two YAML files, one containing your local database credentials, the other containing the RDS database credentials. The YAML files should be stuctured as follows:

    ```yaml
        DRIVER: postgresql
        HOST: your_host
        USER: your_username
        PASSWORD: your_password
        DATABASE: your_database
        PORT: 5432
    ```

3. Create a .env text file in your repository’s root directory in the form:

    ```env
    # RDS Datebase
    rds_db_path = /Users/your_path_to_rds_database.yaml

    # Local Database
    local_db_path = /Users/your_path_to_local_database.yaml

    ## API Store Data
    api_key = `AiCore_API_key`
    number_stores_endpoint = `link_to_AiCore_number_stores_endpoint`
    store_endpoint_template = `link_to_AiCore_store_endpoint_template`

    ## PDF Card Data
    pdf_path = `link_to_AiCore_card_details.pdf`

    ## S3 Date Data
    s3_date_address = `link_to_AiCore_date_details.json`

    ## S3 Products Data
    s3_products_address = `link_to_AiCore_products.csv`
    ```

### Connect to local database

pgAdmin is used to connect to the local database. With pgAdmin installed and running, create a database to upload the data to with these steps:

Within the PgAdmin UI:

- Under the `Object Explorer` panel on the left hand side
- Navigate to the `Databases` sub menu
- Right click on `Databases`
- Select `Create` -> `Database`.

  ![Create -> Database](README_Images/pgadmin_create_database.png)

- Under the `General` tab, enter a name for the new server connection

  ![Database Name](README_Images/pgadmin_database_name.png)

- Click `Save`

### Project Navigation

1. Navigate to the project directory:

    ```bash
    cd multinational-retail-data-centralisation49
    ```

2. Run the following Python Script to download the data and import it into the SQL database:

    ```bash
    python data_ETL_main.py
    ```

    > The following image shows the Entity–relationship model diagram of the created database
    ![sales_data ERD](README_Images/sales_data_ERD.png)

3. Run the following notebook to query the data:

    ```python
    data_query.ipynb
    ```

## Classes and Methods

### DatabaseConnector

A class for connecting to a database, reading credentials from a YAML file, creating a database URL, initialising a SQLAlchemy engine, and performing database operations.

Attributes:

- db_creds_file (str): Path to the YAML file containing database credentials.
- db_engine (sqlalchemy.engine.Engine): SQLAlchemy engine for database operations.

Private Methods:

- __init__(self, db_creds_file: str) -> None: Initialises a DatabaseConnector object.
- __read_db_creds(self) -> dict: Reads and returns the database credentials from the specified YAML file.
- __create_db_url(self) -> URL: Creates a SQLAlchemy database URL based on the provided database credentials.
- __create_psycopg2_url(self) -> str: Creates a PostgreSQL connection URL based on the provided database credentials.

Protected Methods:

- _init_db_engine(self) -> create_engine: Initialises and returns a SQLAlchemy engine using the database URL.

Public Methods:

- list_db_tables(self) -> list: Lists the names of tables in the connected database.
- upload_to_db(self, df: pd.DataFrame, table_name: str) -> None: Uploads a Pandas DataFrame to the specified table in the connected database.
- cast_data_types(self, table_name, column_types) -> None: Casts the data types of columns in a PostgreSQL table based on the provided dictionary of column types.
- add_primary_key(self, table_name, primary_key) -> None: Adds a primary key constraint to a PostgreSQL table.
- add_foreign_key(self, table_name, foreign_keys) -> None: Adds foreign key constraints to a PostgreSQL table based on the provided dictionary of foreign keys.

### DataExtractor

A class for extracting data from various sources such as databases, PDFs, APIs, and S3.

Attributes:

- header (dict): API key header.

Private Methods:

- __init__(self) -> None: Initialises a DataExtractor object.

Public Methods:

- read_rds_table(self, db_connector, table_name: str) -> pd.DataFrame: Reads a table from a relational database and returns the data as a Pandas DataFrame.
- retrieve_pdf_data(self, pdf_link) -> pd.DataFrame: Retrieves data from a PDF link and returns it as a Pandas DataFrame.
- set_api_key(self, api_key) -> None: Sets the API key header.
- list_number_of_stores(self, number_stores_endpoint) -> int: Retrieves the number of stores from an API endpoint.
- retrieve_stores_data(self, store_endpoint, number_of_stores) -> pd.DataFrame: Retrieves store data from an API endpoint for a given number of stores and returns it as a Pandas DataFrame.
- extract_from_s3(self, s3_address) -> pd.DataFrame: Extracts data from an S3 bucket based on the provided S3 address and returns it as a Pandas DataFrame.

### DataLoader

A class for uploading and configuring tables in a PostgreSQL database.

Private Methods:

- __init__(self, db_path: str) -> None: Initialises a DataLoader instance.

Public Methods:

- upload_and_configure_table(self, data_frame: pd.DataFrame, table_name: str, column_types: Dict[str, str], primary_key: Optional[str] = None, foreign_keys: Optional[Dict[str, str]] = None) -> None: Uploads a DataFrame to a specified table and configures the table with given column types, primary key, and foreign keys.
- store_column_types() -> Dict[str, str]: Returns column types for a store table.
- card_column_types() -> Dict[str, str]: Returns column types for a card table.
- products_column_types() -> Dict[str, str]: Returns column types for a products table.
- date_column_types() -> Dict[str, str]: Returns column types for a date table.
- user_column_types() -> Dict[str, str]: Returns column types for a user table.
- orders_column_types() -> Dict[str, str]: Returns column types for a orders table.
- orders_foreign_keys() -> Dict[str, str]: Returns foreign keys for orders table.

### DataCleaning

A class containing static methods for cleaning and preprocessing various types of data.

Public Methods:

- clean_user_data(user_data_df: pd.DataFrame) -> pd.DataFrame: Clean and preprocess user data DataFrame.
- clean_card_data(card_data_df: pd.DataFrame) -> pd.DataFrame: Clean and preprocess card data DataFrame.
- called_clean_store_data(store_data_df: pd.DataFrame) -> pd.DataFrame: Clean and preprocess store data DataFrame.
- convert_product_weights(product_data_df: pd.DataFrame) -> pd.DataFrame: Convert product weights to a standardised unit (kilograms).
- clean_products_data(product_data_df: pd.DataFrame) -> pd.DataFrame: Clean and preprocess product data DataFrame.
- clean_orders_data(orders_data_df: pd.DataFrame) -> pd.DataFrame: Clean and preprocess orders data DataFrame.
- clean_date_data(date_data_df: pd.DataFrame) -> pd.DataFrame: Clean and preprocess date data DataFrame.

## File structure of the project

The project is built around three classes and the Python files needed to download the data:

```bash
.
├── AiCore-Project-MRDC-env.yaml
├── data_ETL_main.py
├── data_query.ipynb
├── README.md
├── __pycache__
├── classes
│   ├── __init__.py
│   ├── __pycache__
│   ├── data_cleaning.py
│   ├── data_extraction.py
│   ├── data_upload.py
│   └── database_utils.py
├── creds_local.yaml
├── creds_rds.yaml
├── .gitignore
└── .env
```

## Tools Used

- Visual Studio Code: Code editor used for development.
- Python: Programming language used for the game logic.
  - pandas: Software library written for the Python programming language for data manipulation and analysis
  - PyYAML: YAML parser and emitter for Python
  - sqlalchemy: Open-source SQL toolkit and object-relational mapper
  - tabula-py: Python wrapper of tabula-java, which can read tables in a PDF
  - psycopg2: PostgreSQL database adapter for the Python programming language
  - requests: Python HTTP library allows users to send HTTP requests to a specified URL.
  - boto3: Boto3 is an AWS SDK for Python that enables developers to integrate their Python applications, libraries, or scripts with AWS services such as Amazon S3, Amazon EC2, and Amazon DynamoDB
  - Decouple: helps you to organise your settings so that you can change parameters without having to redeploy your app.
- Git: Version control system for tracking changes in the project.
- GitHub: Hosting platform for version control and collaboration.
- PostgreSQL: Open-source relational database management system
- pgAdmin4: Administration and development platform for PostgreSQL
- Postgres.app: PostgreSQL installation packaged as a standard Mac app
- Amazon Web Services: cloud computing services
- AiCore: Educational programme for tasks and milestones used for development progression

## Troubleshooting

If you encounter any issues during the installation or setup process, please open an issue in the repository.
