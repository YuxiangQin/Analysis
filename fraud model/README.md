# Driver-Fraud-Anomoly-Predictive-Model  
The Driver Fraud Anomaly Predictive Model is a Machine Learning model that reads data from a BigQuery table, cleans the data, trains a K-means clustering model, analyzes and aggregates data based on driver groups and sends out an email with the analysis results.

## Key Resources
JIRA Ticket [LINK](https://jira.walmart.com/browse/BSCEDA-9689).  
BigQuery Schedular [LINK](https://console.cloud.google.com/bigquery/scheduled-queries/locations/us/configs/672376db-0000-284b-90ec-883d24ff25f8/runs?project=wmt-tebi).  
Confluence Page [LINK](https://confluence.walmart.com/display/BSCEDA/Fraud+and+Abuse%3A+On+Trip+Fraud+Model+and+Anomaly+Detection).

## Installation
### Access Needed
1. For `wmt-lmd-data-science-prod`, ask access from **Triman Kaur** (triman.kaur@walmart.com).
2. AD group `gcp-tebi-reader`, for below tables:
    - `wmt-tebi.jerome.LMD_DISPATCHED_PO_WITH_MISSING`
    - `wmt-tebi.Prashanth_AE_FY24.Driver_DroppedTrips`
    - `wmt-tebi.RogerQin.driver_fraud_anomaly_detection_base_table` 
3. `wmt-edw-prod.WW_CUSTOMER_DL_SECURE.CUST_PROFL`, refer process in [this page](https://confluence.walmart.com/display/DSIC/Request+Access+Customer+Datasets). 

### Python Environment
To set up the Python environment and install the necessary packages, you can use the provided `requirements.txt` file. Run the following command:

```bash
pip install -r requirements.txt
```

The script also depends on the `mlutils` package for the connector to connect to the BigQuery database. This package is expected to be present in the Element platform.

## Configuration
The script needs a config file named 'config.json' in the same directory, which includes all necessary inputs for the script. The 'config.json' file should include the following keys:

- `bq_connector`: String, BigQuery connector to use in Element environment.

- `bq_query`: String, BigQuery SQL query to fetch data.

- `data_type_convert`: Dictionary, maps data types (like "int", "datetime64[ns]") to the corresponding list of column names that should be converted to these types.

- `top_deactivate_reasons`: List, top reasons for deactivating a driver's account.

- `ratio_mapping`: Dictionary, defines the ratios that should be calculated from the data. Each key-value pair represents a ratio, where the key is the denominator and the value is an array of columns that serve as numerators.

- `new_features`: List, names of new features to be passed to K-means model.

- `all_features`: List, names of all features to be passed to K-means model.

- `n_clusters`: Integer, number of clusters to be formed in K-means model.

- `columns_to_analyze`: List, names of columns to be included in the analysis.

- `clusters_to_share`: Integer, number of clusters to be shared, with smallest driver counts. 

- `log_name`: String, name of the log email subject.

- `log_sender`: String, email address from which the logs will be sent.

- `log_to`: String, email address to which the logs will be sent.

## File description
- `config.json`: This file contains all changable inputs.
- `model_deployment.py`: The script will execute the following steps:
    1. It reads the configuration file `config.json`.
    2. It attempts to fetch data from a BigQuery table based on the query provided in the configuration file.
    3. If the data fetch is successful, it cleans the data based on rules defined in the `clean_data()` function.
    4. It then trains two K-means clustering models on the cleaned data, one using new features and one using all features.
    5. The script then analyzes and aggregates data based on driver groups.
    6. It creates an Excel file with the analysis results.
    7. Finally, it sends out an email with the analysis results as an attachment. If the data fetch fails, the script sends out an email notification indicating the failure.
- `prod_data_pipeline.sql`: SQL script to create base table.  
- `Model develop.ipynb`: Jupyter Notebook contains exploratory data analysis (EDA) and decisions made during model development such as the selection of 'K' and the exclusion of certain features. 
- `element_run.ipynb`: Jupyter Notebook contains scripts that test functionality in Element.
- `requirements.txt`: This file contains a list of all packages required to run the script. 

## Output Files
The script generates Excel file with following sheets:

- `Summary_New_Features`: This sheet contains the mean of metrics for each driver group. Data is derived from model that was trained using the new features.

- `Dtl_Least{X}Clstrs_NewFtrs`: This sheet contains driver level detail for the driver groups with least `{X}` driver counts. Data is derived from model that was trained using the new features.

- `Summary_All_Features`: This sheet contains the mean of metrics for each driver group. Data is derived from model that was trained using all the available features.

- `Dtl_Least{X}Clstrs_AllFtrs`: This sheet contains driver level detail for the driver groups with least `{X}` driver counts. Data is derived from models that were trained using all the available features.

Here, `{X}` is a placeholder for the number of clusters to share, as defined in the configuration file.