# Multinational Retail Data Centralisation

## Table of Contents
- [Description of the project](#description-of-the-project)
    - [What the project does](#what-the-project-does)
    - [Aim of the project](#aim-of-the-project)
    - [Lessons learned](#lessons-learned)
- [Installation instructions](#installation-instructions)
- [Usage instructions](#usage-instructions)
- [Classes and Methods](#classes-and-methods)
- [File structure of the project](#file-structure-of-the-project)
- [Tools Used](#tools-used)
- [Troubleshooting](#troubleshooting)
- [License information](#license-information)

## Description of the project
A multinational company sells various goods across the globe. Currently, their sales data is spread across many different data sources, making it not easily accessible or analysable by current team members. To become more data-driven, the organisation would like to make its sales data accessible from one centralised location.
    1. The first goal is to produce a system that stores the current company data in a database so that it's accessed from one centralised location and acts as a single source of truth for sales data.
    2. The second goal is to query the database to get up-to-date metrics for the business.

This project includes a set of Python scripts and classes for processing and cleaning various types of data, including user data, card data, store data, product data, order data, and date data. Each data type has a corresponding class in the codebase, and static methods within these classes handle specific cleaning and preprocessing tasks.

Data has been stored in different sources, including:
    - The historical data of users is currently stored in an AWS RDS database in the cloud.
    - The users' card details are stored in a PDF document in an AWS S3 bucket.
    - The store data can be retrieved through the use of an API.
    - The information for each product the company sells is stored in CSV format in an S3 bucket on AWS.
    - This table, which acts as the single source of truth for all orders the company has made in the past, is stored in a database on AWS RDS.
    - The final source of data is a JSON file containing the details of when each sale happened and related attributes.

### What the project does
A certificate is issued for demonstrating proficiency in techniques related to data handling, including SQL, database management, data manipulation, and data retrieval using APIs.

### Aim of the project
This project aims to test my knowledge of Python programming language, git and GitHub, and the command interface. 
The project is designed to challenge me to refactor and optimise the code while documenting my experience.

### Lessons learned

#### Data Extraction
- Set up and connected to a database engine
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
- Rearrange the columns in a DataFrame
- Used for loops to iterate over a DataFrame with a dictionary
- Reset the index of a DataFrame

#### Data Load
- Connected to a PostgreSQL database

#### General
- Created classes to encapsulate the code
- Defined functions to abstract code
- Set up conda environments to isolate the project dependencies
- Created try except blocks for error handling
- Used descriptive names for methods and variables to enhance code readability.
- Eliminated code duplication; identified repeated code blocks and refactored them into separate methods or functions.
- Ensured that each method has a single responsibility, focusing on a specific task, based on the Single Responsibility Principle (SRP).
- Wrote access modifiers; making methods private or protected when intended for internal use within the class and not externally accessible.
- Used the if __name__ == "__main__" statement to include code blocks that should only run when the script was executed directly, not when imported as a module.
- Wrote with a consistent import order; organised the import statements in a consistent manner: alphabetically, with from statements before import statements to maintain readability.
- Minimised nested loops to improve code efficiency and reduce complexity.
- Imported only the specific methods or classes needed from a module to enhance code clarity and prevent naming conflicts.
- Wrote consistent Docstrings: Providing clear and consistent docstrings for all methods, explaining their purpose, parameters, return values, and errors raised.
- Added type annotations to method signatures to improve code maintainability and catch type-related errors during development

- How to create static methods. --Decorators ??????????

## Installation instructions
To use the data processing and cleaning functionality provided by this project, follow these steps:

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
1. Open the terminal.
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
1. Open the terminal.
2. Install git:
```bash
sudo dnf install git
```

3. Clone the repository to your local machine:
```bash
git clone https://github.com/ChefData/multinational-retail-data-centralisation49
```

## Usage instructions

Follow these instructions to set up and install the project on your local machine.

> [!NOTE]
> Make sure you have the following installed:
>   - Python (version 3.11)
>   - Conda (optional but recommended)
>   - PostgreSQL
>   - pgAdmin4

1. Initialise a new database locally within pgAdmin4 to store the extracted data. 
    - Set up the new database and name it sales_data.
    - This database will store all the company information once you extract it from various data sources.

2. Create a folder within the project directory called do_not_track

3. Within the folder do_not_track, create a file called pg_creds.yaml containing the local database credentials. They are as follows:
    - DRIVER: postgresql
    - HOST: localhost
    - USER: your_username
    - PASSWORD: your_password
    - DATABASE: sales_data
    - PORT: 5432

3. Within the folder do_not_track, create a file called db_creds.yaml containing the database credentials. They are as follows:
    - DRIVER: postgresql
    - HOST: ?????????
    - USER: ?????????
    - PASSWORD: ?????????
    - DATABASE: postgres
    - PORT: 5432

4. Navigate to the project directory:
```bash
cd multinational-retail-data-centralisation49
```

5. Create a Conda Virtual Environment to isolate the project dependencies (Optional but Recommended)
```bash
conda create -n AiCore-Project-MRDC python=3.11 ipykernel pandas PyYAML sqlalchemy postgresql tabula-py psycopg2 requests boto3
```

Or import the conda environment from the supplied YAML file
```bash
conda env create -f AiCore-Project-MRDC-env.yml
```

6. Activate the virtual environment:
- On Windows:
```bash
activate AiCore-Project-MRDC
```

- On macOS and Linux:
```bash
conda activate AiCore-Project-MRDC
```

9. You must be logged into the AWS CLI before retrieving the data from the S3 bucket.
* Open a terminal or command prompt on your local machine
* Run the following command to start the AWS CLI configuration process: 
```bash
aws configure
```
    
* You will be prompted to enter the following information:
    * AWS Access Key ID: Enter the access key ID you obtained during the access key generation process
    * AWS Secret Access Key: Enter the secret access key corresponding to the access key ID you provided
    * Default region name: Specify the default AWS region you want to use for AWS CLI commands. In our case, we will use eu-west-1, as this region is geographically close to the UK.
    * Default output format: Choose the default output format for AWS CLI command results. You can enter JSON, text, table, or YAML. The default format is typically JSON, which provides machine-readable output. If you enter nothing (press Enter) it will default to JSON.
* After entering the required information, press Enter
* To verify that the configuration was successful, run the following command: 
```bash
aws configure list. 
```

This command will display the configuration settings, including the access key ID, secret access key (partially masked), default region, and default output format. Make sure the displayed values match the credentials you provided during the configuration.

9. Run the following Python Scripts to download the data and import it into the SQL database:
```bash
python ETL_rds_user.py
python ETL_rds_orders.py
python ETL_s3_date.py
python ETL_s3_products.py
python ETL_pdf_card.py
python ETL_api_store.py
```

## Classes and Methods

### DatabaseConnector
A class for connecting to a database, reading credentials from a YAML file, creating a database URL, initialising a SQLAlchemy engine, and performing database operations.

#### Attributes
- db_creds_file (str): Path to the YAML file containing database credentials.
- db_engine (sqlalchemy.engine.Engine): SQLAlchemy engine for database operations.

#### Private Methods
- __init__(self, db_creds_file: str) -> None: Initialises a DatabaseConnector object.
- __read_db_creds(self) -> dict: Reads and returns the database credentials from the specified YAML file.
- __create_db_url(self) -> URL: Creates a SQLAlchemy database URL based on the provided database credentials.

#### Protected Methods
- _init_db_engine(self) -> create_engine: Initialises and returns a SQLAlchemy engine using the database URL.
    
#### Public Methods
- list_db_tables(self) -> list: Lists the names of tables in the connected database.
- upload_to_db(self, df: pd.DataFrame, table_name: str) -> None: Uploads a Pandas DataFrame to the specified table in the connected database.

### DataExtractor
A class for extracting data from various sources such as databases, PDFs, APIs, and S3.

#### Attributes
- header (dict): API key header.

#### Private Methods
- __init__(self) -> None: Initialises a DataExtractor object.
    
#### Public Methods
- read_rds_table(self, db_connector, table_name: str) -> pd.DataFrame: Reads a table from a relational database and returns the data as a Pandas DataFrame.
- retrieve_pdf_data(self, pdf_link) -> pd.DataFrame: Retrieves data from a PDF link and returns it as a Pandas DataFrame.
- set_api_key(self, api_key) -> None: Sets the API key header.
- list_number_of_stores(self, number_stores_endpoint) -> int: Retrieves the number of stores from an API endpoint.
- retrieve_stores_data(self, store_endpoint, number_of_stores) -> pd.DataFrame: Retrieves store data from an API endpoint for a given number of stores and returns it as a Pandas DataFrame.
- extract_from_s3(self, s3_address) -> pd.DataFrame: Extracts data from an S3 bucket based on the provided S3 address and returns it as a Pandas DataFrame.

### DataCleaning
A class containing static methods for cleaning and preprocessing various types of data.

#### Public Methods
- clean_user_data(user_data_df: pd.DataFrame) -> pd.DataFrame: Clean and preprocess user data DataFrame.
- clean_card_data(card_data_df: pd.DataFrame) -> pd.DataFrame: Clean and preprocess card data DataFrame.
- called_clean_store_data(store_data_df: pd.DataFrame) -> pd.DataFrame: Clean and preprocess store data DataFrame.
- convert_product_weights(product_data_df: pd.DataFrame) -> pd.DataFrame: Convert product weights to a standardised unit (kilograms).
- clean_products_data(product_data_df: pd.DataFrame) -> pd.DataFrame: Clean and preprocess product data DataFrame.
- clean_orders_data(orders_data_df: pd.DataFrame) -> pd.DataFrame: Clean and preprocess orders data DataFrame.
- clean_date_data(date_data_df: pd.DataFrame) -> pd.DataFrame: Clean and preprocess date data DataFrame.

## File structure of the project
The project is built around three classes and the Python files needed to download the data:

multinational-retail-data-centralisation49/
├── classes/
│ ├── database_utils.py
│ ├── data_extraction.py
│ └── data_cleaning.py
├── scripts/
│ ├── ETL_rds_user.py
│ ├── ETL_rds_orders.py
│ ├── ETL_s3_date.py
│ ├── ETL_s3_products.py
│ ├── ETL_pdf_card.py
│ └── ETL_api_store.py
├── do_not_track/ 
│ ├── pg_creds.yaml
│ └── db_creds.yaml
├── docs/
│ └── AiCore-Project-MRDC-env.yaml
├── .gitignore
└── README.md

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
- Git: Version control system for tracking changes in the project.
- GitHub: Hosting platform for version control and collaboration.
- PostgreSQL: Open-source relational database management system
- pgAdmin4: Administration and development platform for PostgreSQL
- Postgres.app: PostgreSQL installation packaged as a standard Mac app
- Amazon Web Services: cloud computing services
- AiCore: Educational programme for tasks and milestones used for development progression

## Troubleshooting
If you encounter any issues during the installation or setup process, please open an issue in the repository.

## License information
This project is not licensed