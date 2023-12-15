# Multinational Retail Data Centralisation

## Table of Contents
- Description of the project
    - What the project does
    - Aim of the project
    - Lessons learned
- Installation instructions
- Usage instructions
- File structure of the project
- License information

- [Installation](#installation)
- [Usage](#usage)
- [Classes and Methods](#classes-and-methods)
- [Contributing](#contributing)
- [License](#license)

## Description of the project
A multinational company sells various goods across the globe. Currently, their sales data is spread across many different data sources making it not easily accessible or analysable by current members of the team. In an effort to become more data-driven, the organisation would like to make its sales data accessible from one centralised location.
    1. The first goal is to produce a system that stores the current company data in a database so that it's accessed from one centralised location and acts as a single source of truth for sales data.
    2. The second goal is to query the database to get up-to-date metrics for the business.

This project includes a set of Python scripts and classes for processing and cleaning various types of data, including user data, card data, store data, product data, orders data, and date data. Each data type has a corresponding class in the codebase, and static methods within these classes handle specific cleaning and preprocessing tasks.

Data has been stored in different sources including:
    - The historical data of users is currently stored in an AWS RDS database in the cloud.
    - The users card details are stored in a PDF document in an AWS S3 bucket.
    - The store data can be retrieved through the use of an API.
    - The information for each product the company currently sells is stored in CSV format in an S3 bucket on AWS.
    - This table which acts as the single source of truth for all orders the company has made in the past is stored in a database on AWS RDS.
    - The final source of data is a JSON file containing the details of when each sale happened, as well as related attributes.

### What the project does
                This project requires the programmer to:
                    1. Set up the environment
                    2. Create the variables for the game
                    3. Check if the guessed character is in the word
                    4. Create the game class
                    5. Code the logic of the game
                    6. Refactor and optimise the code
                    7. Document the experince

### Aim of the project
                The aim of this project is to test my knowledge in the python programming language, in git and GitHub, and the command interface. The project is designed to challenge me to refactor and optimise the code, while documenting my experince.

### Lessons learned
                - Created a list and assigned the list to a variable
                - Imported the module random and used the random.choice method on the list to generate a new variable
                - Asked the user for an input and checked the validaty of the input using the len() function and .isalpha method
                - Used an if statement to communcate validaty of input back to the user
                - Updated the GitHub repository with the latest code changes from your local project
                - Refactored and optimised code to inculde meaningfull names and eliminate code duplication
                - Created a while loop to continuously ask for user input if input was entered incorrectly
                - Defined functions to abstract code
                - Created a class to encapsulate the code
                - Initiated the following attributes
                    - word: The word to be guessed, picked randomly from the word_list.
                    - word_guessed: list - A list of the letters of the word, with _ for each letter not yet guessed. For example, if the word is 'apple', the word_guessed list would be ['_', '_', '_', '_', '_']. If the player guesses 'a', the list would be ['a', '_', '_', '_', '_']
                    - num_letters: int - The number of UNIQUE letters in the word that have not been guessed yet
                    - num_lives: int - The number of lives the player has at the start of the game.
                    - word_list: list - A list of words
                    - list_of_guesses: list - A list of the guesses that have already been tried. Set to an empty list initially
                - Used an if statement that checks if the guess is in the word
                - Extended an if statement to include an elif statment that checks if the guess is already in the list_of_guesses
                - Used the .append() method to append the guess to the list_of_guesses
                - Used a for-loop to replace the underscore(s) in the word_guessed with the letter guessed by the user
                - Extended an if statment to include an else statment that defines what happens if the guess is not in the word
                - Create an instance of the class

Refactoring will be a continuous and constant process, but this is the time to really scrutinise your code.
You can use the following list to make improvements:
    - Meaningful Naming: Use descriptive names for methods and variables to enhance code readability. For example, create_list_of_website_links() over links() and use for element in web_element_list instead of for i in list.
    - Eliminate Code Duplication: Identify repeated code blocks and refactor them into separate methods or functions. This promotes code reusability and reduces the likelihood of bugs.
    - Single Responsibility Principle (SRP): Ensure that each method has a single responsibility, focusing on a specific task. If a method handles multiple concerns, split it into smaller, focused methods.
    - Access Modifiers: Make methods private or protected if they are intended for internal use within the class and not externally accessible
    - Main Script Execution: Use the if __name__ == "__main__": statement to include code blocks that should only run when the script is executed directly, not when imported as a module
    - Consistent Import Order: Organize import statements in a consistent manner, such as alphabetically, and place from statements before import statements to maintain readability
    - Avoid Nested Loops: Minimize nested loops whenever possible to improve code efficiency and reduce complexity
    - Minimal Use of self: When writing methods in a class, only use self for variables that store information unique to each object created from the class. This helps keep the code organized and ensures that each object keeps its own special data separate from others.
    - Avoid import *: Import only the specific methods or classes needed from a module to enhance code clarity and prevent naming conflicts
    - Consistent Docstrings: Provide clear and consistent docstrings for all methods, explaining their purpose, parameters, and return values. This aids code understanding for other developers.
    - Type Annotations: Consider adding type annotations to method signatures, variables, and return values to improve code maintainability and catch type-related errors during development


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
2. If you don't have Git installed, you can install it using [Homebrew](https://brew.sh/):
    ```bash
    /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
    brew install git
    ```
3. Clone the repository to your local machine:
    ```bash
    git clone https://github.com/ChefData/multinational-retail-data-centralisation49
    ```

#### Linux
1. Open the terminal.
2. Install Git:
    - On Ubuntu or Debian-based systems:
        ```bash
        sudo apt-get update
        sudo apt-get install git
        ```
    - On Fedora:
        ```bash
        sudo dnf install git
        ```
3. Clone the repository to your local machine:
    ```bash
    git clone https://github.com/ChefData/multinational-retail-data-centralisation49
    ```

## Usage instructions

1. Initialise a new database locally within pgadmin4 to store the extracted data. 
    - Set up the new database and name it sales_data.
    - This database will store all the company information once you extract it for the various data sources.
2. Create a file called pg_creds.yaml containing the local database credentials, they are as follows:
    - DRIVER: postgresql
    - HOST: localhost
    - USER: your_username
    - PASSWORD: your_password
    - DATABASE: sales_data
    - PORT: 5432
3. Create a file called db_creds.yaml containing the database credentials, they are as follows:
    - ??????????
4. Navigate to the project directory:
    ```bash
    cd multinational-retail-data-centralisation49
    ```
5. Create a Virtual Environment to isolate the project dependencies (Optional but Recommended):
    ```bash
    python -m venv venv
    ```
    ```bash
    conda create -n AiCore-Project-MRDC python=3.11 ipykernel pandas PyYAML sqlalchemy postgresql tabula-py psycopg2 requests boto3
    ```
6. Activate the virtual environment:
    - On Windows:
        ```bash
        .\venv\Scripts\activate
        ```
    - On macOS and Linux:
        ```bash
        source venv/bin/activate
        ```
7. Install the required dependencies using pip:
    ```bash
    pip install -r requirements.txt
    ```
8. You will need to be logged into the AWS CLI before you retrieve the data from the S3 bucket.
    1. Open a terminal or command prompt on your local machine
    2. Run the following command to start the AWS CLI configuration process: 
        ```bash
        aws configure
        ```
    3. You will be prompted to enter the following information:
        * AWS Access Key ID: Enter the access key ID you obtained during the access key generation process
        * AWS Secret Access Key: Enter the secret access key corresponding to the access key ID you provided
        * Default region name: Specify the default AWS region you want to use for AWS CLI commands. In our case, we will use eu-west-1, as this region is geographically close to the UK.
        * Default output format: Choose the default output format for AWS CLI command results. You can enter JSON, text, table, or YAML. The default format is typically JSON, which provides machine-readable output. If you enter nothing (press Enter) it will default to JSON.
    4. After entering the required information, press Enter
    5. To verify that the configuration was successful, run the following command: 
            ```bash
            aws configure list. 
            ```
        This command will display the current configuration settings, including the access key ID, secret access key (partially masked), default region, and default output format. Make sure the displayed values match the credentials you provided during the configuration.

9. Run Your Python Scripts
    ```bash
    python export_data_rds_user.py
    python export_data_rds_orders.py
    python export_data_s3_date.py
    python export_data_s3_products.py
    python export_data_pdf_card.py
    python export_data_api_store.py
    ```


## File structure of the project
        The project is built around 4 milestone python files which increase in complexity to reach the finished game

        hangman442/
        |
        |-- milestone_2.py
        |
        |-- milestone_3.py
        |
        |-- milestone_4.py
        |
        |-- hangman.py
        |
        |-- README.md

## Tools Used
- Python: Programming language used for the game logic.
- Git: Version control system for tracking changes in the project.
- GitHub: Hosting platform for version control and collaboration.
- Visual Studio Code: Code editor used for development.
        - URL ("https://www.mit.edu/~ecprice/wordlist.10000"): Word list used to populate the project
- AiCore: Educational programme for tasks and milestones used for development progression

## License information
This project is not licensed





## Installation

To use the data processing and cleaning functionality provided by this project, follow these steps:

1. Clone the repository to your local machine:

   ```bash
   git clone https://github.com/yourusername/data-processing-project.git
