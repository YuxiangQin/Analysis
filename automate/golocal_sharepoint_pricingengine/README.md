# GoLocal Sharepoint Scraper
This script extracts data from a SharePoint Excel file, which contains pricing engine transition cutoff dates. It then cleans the data and stores it in a BigQuery table. The process can be easily replicated for similar tasks.

## Installation

To set up the Python environment and install the necessary packages, you can use the provided `requirements.txt` file. Run the following command:

```bash
pip install -r requirements.txt
```

## SharePoint User Authentification
### Element environment
User account used to connect with sharepoint file, username saved in `configs.json`, password saved using Element storage.  
To ensure password security, it's recommended to run the following code in an Element notebook to update your password:
```python
from mlutils import storage
storage.update_secret("password", "YOUR PASSWORD HERE")
```
For more details on saving secrets in notebooks, refer to this [guide](https://console.dx.walmart.com/element-docs/notebooks/how_to_save_secrets_in_notebooks.html).  

### Local machine
When developing in local environment, password saved in `.env` file, which is ignored by `.gitignore` and not uploaded in remote repository:  
```txt
PASSWORD="YOUR PASSWORD HERE"
```

## File description
- `configs.json`: This file contains all changable inputs.
- `sharepoint_pricingengine.py`: This script extracts data from sharepoint, cleans it and stores the data to BigQuery table. It is not generally necessary for users to modify this script unless there are changes in the Excel file structure or major alterations in the process.

## Configuration

The script needs a config file named 'configs.json' in the same directory, which includes all necessary inputs for the script. The 'configs.json' file should include the following keys:

- username: The username used to authenticate to the SharePoint site.
- url: The URL of the SharePoint site.
- site_path: The path of the SharePoint site where the file is located.
- folder_path: The folder path where the file is located.
- file_name: The name of the file to be downloaded.
- sheet_nm: The name of the sheet to be read from the downloaded Excel file.
- unique_key: A list contains unique key used for checking missing data and performing data deduplication.
- data_type_convert: A dictionary mapping column names to their desired data types.
- log_name: Name of the log.
- log_sender: Sender email address for the log.
- log_to: Recipient email address for the log.
- log_with_attachment: Additional email body content for logs with attachments.
- bq_connector: BigQuery connector to use in Element environment.
- bq_table_nm: Name of the BigQuery table where the data will be uploaded.

**Note that app path '/home/jupyter/automation/golocal_sharepoint_pricingengine/' is hard coded in the script, need to be modified by need.**

## Functionality

The script includes the following key functions:

- `get_data()`: This function connects to the SharePoint site, downloads the specified Excel file, and loads it into a Pandas DataFrame.

- `clean_data(df)`: This function performs data cleaning on the DataFrame. It includes replacing a specific value with a maximum date, checking for missing values, converting data types, and checking for duplicates. It returns missing and duplicate rows if any.

- `create_excel_file(df_dict)`: This function creates an Excel file from a dictionary of DataFrames and returns it as an email attachment.

- `to_bq(df)`: This function uploads a DataFrame to a BigQuery table, overwriting existing data if the table already exists.

- `main()`: This is the main function that calls all other functions and sends an email log with the results of the execution.

Each function is well-documented with docstrings explaining their purpose and parameters.
