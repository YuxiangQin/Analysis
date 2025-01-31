import os.path
import argparse
import json
import pandas as pd
import pytz
from datetime import datetime

# sharepoint connection
from office365.runtime.auth.authentication_context import AuthenticationContext
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.files.file import File

#Email Utils
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib
import io

#Element password storage
try:
    from mlutils import storage
    PASSWORD = storage.get_secret("password_for_sharepoint_bq_transfer")
except ImportError:
    # read from local .env file
    from dotenv import load_dotenv
    import os
    load_dotenv()
    PASSWORD = os.getenv('PASSWORD')

# Bigquery
from google.cloud import bigquery

try:
    from mlutils import dataset, connector # Element environment connectors
    APP_PATH = '/home/jupyter/automation/inhome/'
except ImportError:
    APP_PATH = ""

# pull today's date
EMAIL_DATE = datetime.now(pytz.timezone('America/Los_Angeles')).strftime("%Y-%m-%d")
    
def get_config(APP_PATH, config_file):
    '''Return config file'''
    config_path = os.path.join(APP_PATH, config_file)
    with open(config_path, 'r') as file:
        config = json.load(file)
    return config

# default config when importing as a package
CONFIG = get_config(APP_PATH, 'config_catchment.json')

def get_data():
    '''Read data from sharepoint, save and return in a DataFrame'''
    try:
        # load variables from CONFIG
        url = CONFIG['url']
        username = CONFIG['username']
        site_path = CONFIG['site_path']
        folder_path = CONFIG['folder_path']
        file_name = CONFIG['file_name']
        file_path = os.path.join(site_path, folder_path, file_name).replace("\\", "/") # replace sep when developing in windows environment
        sheet_nms = CONFIG['sheet_names']
        # read file
        ctx_auth = AuthenticationContext(url)
        ctx_auth.acquire_token_for_user(username, PASSWORD)
        ctx = ClientContext(url, ctx_auth)
        file = ctx.web.get_file_by_server_relative_url(file_path).get().execute_query()
        content = file.read()
        # loop over sheets and stack them
        dfs = []
        for sheet in sheet_nms:
            df = pd.read_excel(content, sheet_name=sheet, engine="openpyxl")
            dfs.append(df)
        final_df = pd.concat(dfs, axis=0, ignore_index=True)
        return final_df
    except Exception as e:
        log_body = file_name+" - Download Error: " + str(e)
        send_email(create_log(log_body, is_failed=True))
        raise

def create_log(log_body, is_failed=False, attachment=None):
    '''Returns a MIMEMultipart object with log information'''
    msg = MIMEMultipart()
    msg['From'] = CONFIG['log_sender']
    msg['To'] = CONFIG['log_to']
    log_name = CONFIG['log_name']
    if is_failed:
        msg['Subject'] = f'FAIL: {log_name}_{EMAIL_DATE}'
    else:
        msg['Subject'] = f'SUCCESS: {log_name}_{EMAIL_DATE}'
    msg.attach(MIMEText(log_body, 'plain'))
    if attachment is not None:
        msg.attach(attachment)

    return msg

def send_email(msg):
    with smtplib.SMTP('smtp.wal-mart.com') as server:
        server.send_message(msg)

def format_column_name(name):
    # split the name by spaces
    words = name.split(" ")
    # capitalize the first letter of each word and join them together with underscores
    camel_case_name = "_".join(word.capitalize() for word in words)
    return camel_case_name

def clean_data(df):
    '''Clean data set, return missing and duplicate rows if any.'''
    # fromat column names
    df.columns = [format_column_name(col_name) for col_name in df.columns]

    # clean empty rows
    df.dropna(how='all', inplace=True)

    # replace missing value in End_Date column with a far furture date, max possible in pandas is "2262-04-11"
    df['End_Date'] = df['End_Date'].fillna("2261-12-31")
    
    # missing check
    unique_key = CONFIG['unique_key']
    missing_rows = df[df[unique_key].isna().any(axis=1)].copy()
    df.dropna(how='any', subset=unique_key, inplace=True)

    # data type conversion
    type_conv = CONFIG['data_type_convert']
    for col, dtype in type_conv.items():
        if dtype == 'datetime64[ns]':
            df[col] = pd.to_datetime(df[col].fillna(pd.NaT), errors='coerce')
            df[col] = df[col].dt.date # save as date in BigQuery table, other than timestamp
        else:
            df[col] = df[col].astype(dtype)
    
    # duplication check
    duplicated_rows = df[df.duplicated(unique_key, keep=False)].copy()
    duplicated_rows.sort_values(unique_key, inplace=True)
    # drop all duplicates
    df.drop_duplicates(unique_key, keep=False, inplace=True)

    return missing_rows, duplicated_rows

def create_excel_file(df_dict):
    '''
    Creates an Excel file from the provided dataframes and returns an email attachment.

    Parameters:
        df_dict (Dict[str, DataFrame]): Dictionary of DataFrames to include in the Excel file. 
                                         Keys are used as sheet names.
    Returns:
        attachment (MIMEApplication): Email attachment.
    '''
    filename = f"{CONFIG['log_name']}_{EMAIL_DATE}.xlsx"
    attachment = None
    # create attachment when at least one df is not empty
    if any(not df.empty for df in df_dict.values()):
        # write dfs into excel
        with io.BytesIO() as buffer:
            with pd.ExcelWriter(buffer) as writer:
                for sheetname, data in df_dict.items():
                    data.to_excel(writer, sheetname, index=False)
            writer.save()
            buffer_value = buffer.getvalue()
        
        attachment = MIMEApplication(buffer_value)
        attachment['Content-Disposition'] = f'attachment; filename={filename}'
    return attachment

def to_bq(df):
    '''
    Upload a pandas DataFrame to a BigQuery table, overwriting existing data if the table already exists. 

    Parameters:
    df (dataframe): DataFrame to be uploaded.
    Returns:
    tuple: A boolean indicating success (True) or failure (False), and a log message with the timestamp of upload or error details.
    '''
    table_id = CONFIG['bq_table_nm']
    bq_conn = CONFIG['bq_connector']
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE") # overwrite existing data

    try:
        client = connector.get_connector(name=bq_conn)
    except:
        client = bigquery.Client()

    try:
        load_job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        # Wait for load job to complete.
        load_job.result()
        # send log
        log_body = "Uploaded at: "+datetime.now(pytz.timezone('America/Los_Angeles')).strftime("%Y-%m-%d %H:%M:%S")+" PT"
        return(True, log_body)
    except Exception as e:
        log_body = table_id + " - Write Error: " + str(e)
        return(False, log_body)

def main():
    # Argument parser
    global CONFIG
    parser = argparse.ArgumentParser(description='Run script with different config files.')
    parser.add_argument('--config', type=str, required=True, help='The name of the config file.')
    args = parser.parse_args()
    # load config file
    CONFIG = get_config(APP_PATH, args.config)

    df = get_data()
    missing, duplicated = clean_data(df)
    bq_res = to_bq(df)
    is_success, log_body = bq_res
    # send log based on upload to BigQuery result
    if is_success:
        attachment = create_excel_file({'Missing': missing, 'Duplicated': duplicated})
        if attachment is not None:
            log_body += CONFIG['log_with_attachment']
        msg = create_log(log_body, is_failed=False, attachment=attachment)
        send_email(msg)
    else:
        msg = create_log(log_body, is_failed=True)
        send_email(msg)
    
if __name__ == "__main__":
    main()
