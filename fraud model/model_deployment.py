import os
import json
import pytz
from datetime import datetime

import pandas as pd
import numpy as np

# Bigquery
from google.cloud import bigquery

try:
    from mlutils import connector # Element environment connectors
    APP_PATH = '/home/jupyter/Driver-Fraud-Anomaly-Predictive-Model/'
except ImportError:
    APP_PATH = ""
    
# Machine Learning
from sklearn.metrics import silhouette_score
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler

#Email Utils
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib
import io

# pull today's date
EMAIL_DATE = datetime.now(pytz.timezone('America/Los_Angeles')).strftime("%Y-%m-%d")

def get_config(APP_PATH, config_file):
    '''Return config file'''
    config_path = os.path.join(APP_PATH, config_file)
    with open(config_path, 'r') as file:
        config = json.load(file)
    return config


def get_data(config):
    '''Read data from BigQuery table given in config file'''
    query_str = config['bq_query']
    bq_conn = config['bq_connector']
    job_config = bigquery.QueryJobConfig()
    
    try:
        client = connector.get_connector(name=bq_conn)
    except:
        client = bigquery.Client()

    try:
        query_job = client.query(query_str, job_config=job_config)

        # Get the result
        df = query_job.to_dataframe()

        start_dt = df.data_start_dt_pt.min()
        end_dt = df.data_end_dt_pt.min()
        log_body = f"The Driver Fraud Anomaly Predictive Model has successfully processed the data from {start_dt} to {end_dt} PT. Result analysis compiled into attached excel file. It provides detailed and summarized insights on driver groups based on both new and all available features."
        return (True, df, log_body)
    except Exception as e:
        log_body = "Data Read Error: " + str(e)
        return (False, None, log_body)

def clean_data(df, config):
    '''Cleans the data and returns cleaned DataFrame'''
    # deal with total_po_cnt == 0
    # fill with missing_po_cnt first, if missing_po_cnt > 0, then missing_po_pct would be 1
    df.loc[df['total_po_cnt'] == 0, 'total_po_cnt'] = df[df['total_po_cnt'] == 0]['missing_po_cnt']
    # if still 0, then both missing_po_cnt and total_po_cnt are 0, fill with 1 (placholder), so missing_po_pct will be 0
    df.loc[df['total_po_cnt'] == 0, 'total_po_cnt'] = 1
    
    # data type convert
    type_conv = config['data_type_convert']
    for dtype, cols in type_conv.items():
        if dtype == 'datetime64[ns]':
            for col in cols:
                df[col] = pd.to_datetime(df[col].fillna(pd.NaT), errors='coerce')
                df[col] = df[col].dt.date # save as date in BigQuery table, other than timestamp
        else:
            for col in cols:
                df[col] = df[col].astype(dtype)
    
    # deactivate flag within data time range, exluding deleted accounts (likely voluntarily deleted)
    df['deactivated'] = ((df.LAST_DEATVD_TS >= df.data_start_dt_pt) &
                         (df.LAST_DEATVD_TS <= df.data_end_dt_pt) & 
                        (df.DEACTIVATION_TYPE_NM != 'DELETED')) * 1
    
    # create separate column for top deactivation reasons
    for reason in config["top_deactivate_reasons"]:
        df[reason] = ((df['DEACTIVATION_RSN_NM'] == reason) & (df['deactivated'] == 1)).astype(int)
    
    # create ratio columns
    mapping = config["ratio_mapping"]
    
    for level in mapping.keys():
        for metric in mapping[level]:
            df[f'{metric[:-4]}_pct'] = df[metric] / df[level]
            
    return df


def train_k_means(df, config, feature_group_name):
    '''
    Scaled the data and trains k-means model using the number of clusters specified in the configuration, 
    and adds the resulting cluster labels to the original dataframe.
    The function also calculates and returns the silhouette score of the clustering, using 5% of the total data size.
    The output is the modified dataframe, the trained k-means model, and the silhouette score.
    '''
    data = df[config[feature_group_name]]
    # scale the data
    scaler = StandardScaler()
    data_scaled = scaler.fit_transform(data)
    
    kmeans = KMeans(n_clusters=config["n_clusters"], random_state=42).fit(data_scaled)

    df[f'driver_group_{feature_group_name}'] = kmeans.labels_

    silhouette_score_value = silhouette_score(data_scaled, kmeans.labels_, sample_size=int(df.shape[0] * .05), random_state=42)
    
    return df, kmeans, silhouette_score_value


def analyze_driver_groups(df, config, feature_group_name):
    """
    Analyzes and aggregates data based on driver groups.

    This function groups data by specified driver group, calculates mean for specified columns
    and counts the number of drivers in each group. It also identifies the nth smallest clusters 
    and returns detailed data for them.

    Parameters:
    df (pandas.DataFrame): The dataframe to analyze.
    config (dict): A configuration dictionary containing columns to analyze, top deactivate reasons, and number of clusters to share.
    feature_group_name (str): The name of the driver group to analyze.

    Returns:
    tuple: A tuple containing two dataframes. The first dataframe contains aggregated data 
    (mean and count), transposed for better readability. The second dataframe contains 
    detailed data for the nth smallest clusters.
    """
    output_cols = ['deactivated'] + config["top_deactivate_reasons"] + config["columns_to_analyze"]
    feature_driver_group = f'driver_group_{feature_group_name}'
    mean_df = df.groupby(feature_driver_group)[output_cols].mean()
    count_df = df.groupby(feature_driver_group)['DRVR_USER_ID'].count().rename('driver_count')
    result_df = pd.concat([count_df, mean_df], axis=1)

    # share details in certain clusters
    least_clusters = result_df['driver_count'].nsmallest(config["clusters_to_share"]).index 
    detail_df = df[df[feature_driver_group].isin(least_clusters)].sort_values(feature_driver_group)
    
    return result_df.T.reset_index().rename(columns={'index': feature_driver_group}), detail_df

# Email related functions
def create_excel_file(df_dict, config):
    '''
    Creates an Excel file from the provided dataframes and returns an email attachment.

    Parameters:
        df_dict (Dict[str, DataFrame]): Dictionary of DataFrames to include in the Excel file. 
                                         Keys are used as sheet names.
    Returns:
        attachment (MIMEApplication): Email attachment.
    '''
    filename = f"{config['log_name']}_{EMAIL_DATE}.xlsx"
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


def create_log(log_body, config, is_success=True, attachment=None):
    '''Returns a MIMEMultipart object with log information'''
    msg = MIMEMultipart()
    msg['From'] = config['log_sender']
    msg['To'] = config['log_to']
    log_name = config['log_name']
    if is_success:
        msg['Subject'] = f'SUCCESS: {log_name}_{EMAIL_DATE}'
    else:
        msg['Subject'] = f'FAIL: {log_name}_{EMAIL_DATE}'
        
    msg.attach(MIMEText(log_body, 'plain'))
    
    if attachment is not None:
        msg.attach(attachment)

    return msg


def send_email(msg):
    with smtplib.SMTP('smtp.wal-mart.com') as server:
        server.send_message(msg)

def main():
    config = get_config(APP_PATH, "config.json")
    print("Getting data...")
    is_success, df, log_body = get_data(config)

    # send log based on get_data() result
    if is_success:
        print("Data pulled successfully, cleanning data...")
        clean_df = clean_data(df, config)
        # train using both new_features and all features
        print("Training models...")
        trained_df_new, kmeans_new, silhouette_score_value_new = train_k_means(clean_df, config, "new_features")
        trained_df_all, kmeans_all, silhouette_score_value_all = train_k_means(clean_df, config, "all_features")
        print(f"silhouette score using new features: {silhouette_score_value_new}")
        print(f"silhouette score using all features: {silhouette_score_value_all}")
        
        # output result
        result_df_new, detail_df_new = analyze_driver_groups(trained_df_new, config, "new_features")
        result_df_all, detail_df_all = analyze_driver_groups(trained_df_all, config, "all_features")

        # create excel attachment
        X = config['clusters_to_share']

        df_dict = {
            'Summary_New_Features': result_df_new,
            f'Dtl_Least{X}Clstrs_NewFtrs': detail_df_new,
            'Summary_All_Features': result_df_all,
            f'Dtl_Least{X}Clstrs_AllFtrs': detail_df_all
        }
        attachment = create_excel_file(df_dict, config)
        print("Operation complete, send out email with data.")
        msg = create_log(log_body, config, is_success, attachment=attachment)
        send_email(msg)
    else:
        print("Data pull failed, send out failure notification.")
        msg = create_log(log_body, config, is_success)
        send_email(msg)

    
if __name__ == "__main__":
    main()