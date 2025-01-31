# General-Purpose SharePoint Data Extractor
This script is designed to extract data from a SharePoint Excel file and store it in a BigQuery table after cleaning the data. Its general-purpose nature allows it to be easily adapted to similar tasks.

## Table of Contents
- [Installation](#installation)
- [SharePoint User Authentication](#sharepoint-user-authentication)
  - [Element Environment](#element-environment)
  - [Local Environment](#local-environment)
- [Default Element Path and Configuration](#default-element-path-and-configuration)
- [Configuration](#configuration)
- [File Description](#file-description)
- [Functionality](#functionality)

## Installation

To set up the Python environment and install the necessary packages, you can use the provided `requirements.txt` file. Run the following command:

```bash
pip install -r requirements.txt
```

## SharePoint User Authentication
### Element Environment
User account used to connect with sharepoint file, username is stored in the respective configuration files, password saved using Element storage.  
To ensure password security, it's recommended to run the following code in an Element notebook to update your password:
```python
from mlutils import storage
storage.update_secret("password_for_sharepoint_bq_transfer", "YOUR PASSWORD HERE")
```
For more details on saving secrets in notebooks, refer to this [guide](https://console.dx.walmart.com/element-docs/notebooks/how_to_save_secrets_in_notebooks.html).  
**Note**: secret in `storage` package is shared in same Element evironment, it's recommended to give your secret an unique key name to avoid accidentally overwritten by others using the same Element evironment.  

### Local Environment
When developing in local environment, password saved in `.env` file, which is ignored by `.gitignore` and not uploaded in remote repository:  
```txt
PASSWORD="YOUR PASSWORD HERE"
```

## Default Element Path and Configuration

The default path in element is defined by `APP_PATH`, currently set as '/home/jupyter/automation/inhome/'.  
The default configuration file is `config_catchment.json` located in the directory specified by `APP_PATH`. This configuration is loaded when the script is imported as a package.

```python
CONFIG = get_config(APP_PATH, 'config_catchment.json')
```

If your Element path is different, or if you want to use a different default configuration file, make sure to update the `APP_PATH` variable and the filename in the `get_config` function call.

Example:

```python
APP_PATH = "/path/to/your/config/files"
CONFIG = get_config(APP_PATH, 'your_config.json')
```

After updating the `APP_PATH` and `CONFIG`, you can import and use the script as a package in your Python programs. When you run the script directly, you can specify the configuration file with the `--config` flag:

```bash
python your_main_script.py --config your_config.json
```

The `--config` flag should point to a `.json` configuration file within the directory specified by `APP_PATH`. This file should contain all necessary configurations for the script.

## Configuration

The script needs a config file named 'configs.json' in the same directory, which includes all necessary inputs for the script. The 'configs.json' file should include the following keys:

- `username`: The username used to authenticate to the SharePoint site.
- `url`: The URL of the SharePoint site.
- `site_path`: The path of the SharePoint site where the file is located.
- `folder_path`: The folder path where the file is located.
- `file_name`: The name of the file to be downloaded.
- `sheet_names`: A list of sheet names to be read from the downloaded Excel file.
- `unique_key`: A list contains unique key used for checking missing data and performing data deduplication.
- `data_type_convert`: A dictionary mapping column names to their desired data types.
- `log_name`: Name of the log.
- `log_sender`: Sender email address for the log.
- `log_to`: Recipient email address for the log.
- `log_with_attachment`: Additional email body content for logs with attachments.
- `bq_connector`: BigQuery connector to use in Element environment.
- `bq_table_nm`: Name of the BigQuery table where the data will be uploaded.

## File description
- `config_catchment.json`: This file contains all changable inputs for catchment.
- `config_van_count.json`: This file contains all changable inputs for van count.
- `config_new_store_launch.json`: This file contains all changeable inputs for new store launch.
- `sharepoint_inhome.py`: This script extracts data from sharepoint, cleans it and stores the data to BigQuery table. It is not generally necessary for users to modify this script unless there are changes in the Excel file structure or major alterations in the process.
- `development.ipynb`: Jupyter Notebook contains testing scripts. 
- `element_run.ipynb`: Jupyter Notebook contains scripts that got triggered in Element scheduled job.
- `requirements.txt`: This file contains a list of all packages required to run the script. 

## Functionality

The script includes the following key functions:

- `get_data()`: This function connects to the SharePoint site, downloads the specified Excel file, and loads it into a Pandas DataFrame.

- `clean_data(df)`: This function performs data cleaning on the DataFrame. It includes replacing a specific value with a maximum date, checking for missing values, converting data types, and checking for duplicates. It returns missing and duplicate rows if any.

- `create_excel_file(df_dict)`: This function creates an Excel file from a dictionary of DataFrames and returns it as an email attachment.

- `to_bq(df)`: This function uploads a DataFrame to a BigQuery table, overwriting existing data if the table already exists.

- `main()`: This is the main function that calls all other functions and sends an email log with the results of the execution.

Each function is well-documented with docstrings explaining their purpose and parameters.
