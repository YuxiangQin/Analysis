#PACKAGE IMPORTS

#Python Utils
import pandas as pd
import io
import os
import json
from datetime import datetime
import pytz
from string import Template

#Email Utils
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib

# Bigquery
try:
    from mlutils import dataset, connector      # type: ignore
    from mlutils.nbhelper import getArgument    # type: ignore
    APP_PATH = "/home/jupyter/"
except ImportError:
    import google.auth
    from google.cloud.bigquery import Client
    APP_PATH = ""

# UDFs
def get_config(config_file):
    '''load configuration from a json file, returns a dictionary'''
    config_path = os.path.join(APP_PATH, "configs", config_file)
    with open(config_path, 'r') as file:
        res = json.load(file)
    return res

def get_data(file_path):
    '''Return a dataframe from .sql file stored in @file_path'''
    try:
        client = connector.get_connector(name='Google_BigQuery_Connector')
    except:
        creds, project = google.auth.default()
        client = Client(project,creds)

    query_path = os.path.join(APP_PATH, file_path)
    with open(os.path.normpath(query_path), "r") as file:
        query = file.read()

    df = client.query(query).to_dataframe()
    return df

def join_strings(row):
    '''Join strings with comma, skip null values'''
    non_null_values = [str(value) for value in row if pd.notnull(value)]
    return ', '.join(non_null_values)

def clean_emails(text):
    '''Clean leading and trailing spaces then join together with comma'''
    if pd.isnull(text):
        return None
    else:
        return ', '.join([w.strip() for w in text.split(',')])

def clean_data(df):
    df['Chargebacks'] = df[['Late_T', 'Reject_T', 'TCB_T']].apply(join_strings, axis=1)
    df['VendorEmails'] = df['VendorEmails'].apply(clean_emails)
    df['OwnerEmail'] = df['OwnerEmail'].apply(clean_emails)
    return df

def html_template(file_path, mapping):
    '''Returns a html file from @file_path with variables replaced using @mapping'''
    with open(os.path.normpath(file_path), 'r') as file:
        template = Template(file.read())

    return template.safe_substitute(mapping)

def create_email(subject, sender, to_str, cc_str, html_body, attachment=None):
    '''Returns a MIMEMultipart object'''
    msg = MIMEMultipart()
    msg['From'] = sender
    msg['To'] = to_str
    if cc_str is not None:
        msg['Cc'] = cc_str
    msg['Subject'] = subject
    msg.attach(MIMEText(html_body, 'html'))
    if attachment is not None:
        msg.attach(attachment)

    return msg

def send_email(msg):
    with smtplib.SMTP('smtp.wal-mart.com') as server:
        server.send_message(msg)

def process(row, config):
    '''Process on each row to send email alerts'''
    if pd.notnull(row['Comments']): # do nothing
        return row['Comments']
    else:
        mapping = {
            'Vendor_Name': row['VENDOR_NAME'],
            'Chargebacks': row['Chargebacks'],
            'month_year': row['month_year']
        }
        html_body = html_template(config['html_template'], mapping)

        msg = create_email(config['subject'],
                          config['sender'],
                          row['VendorEmails'],
                          row['OwnerEmail'],
                          html_body)
        send_email(msg)
        return 'Success'

def export_excel(df_dict):
    '''Convert DF to Excel (Used by emailing UDFs)'''
    with io.BytesIO() as buffer:
        with pd.ExcelWriter(buffer) as writer:
            for sheetname, data in df_dict.items():
                data.to_excel(writer, sheetname, index=False)
        return buffer.getvalue()

def send_log(df_input, log_config):
    '''Save and send run log'''
    df = df_input[log_config['cols']]
    year_month = f"{df.iloc[0]['year']}-{df.iloc[0]['month']}"
    email_date = datetime.now(pytz.timezone('America/Los_Angeles')).strftime("%Y-%m-%d")
    run_stats = df['Comments'].fillna(" ").value_counts().reset_index()\
        .rename(columns={'index':'Status','Comments':'Vendor'}).to_html(index=False)
    log_mapping = {
        "year_month": year_month,
        "email_date": email_date,
        "run_stats": run_stats
    }
    log_subject = log_config['subject'] + " " + year_month
    html_body = html_template(log_config['html_template'], log_mapping)
    logname = log_config['filename']
    filename = f"{logname}_{email_date}.xlsx"
    attachment = MIMEApplication(export_excel({'Summary': df}))
    attachment['Content-Disposition'] = f'attachment; filename={filename}'
    # save log file
    df.to_excel(os.path.join(os.path.normpath(log_config['filepath']), filename), index=False)
    # send log email
    to_str = log_config['to'] + ', ' + ', '.join([i for i in list(df['OwnerEmail'].unique()) if '@' in str(i)])
    msg = create_email(log_subject, log_config['sender'], to_str, log_config['cc'],
                       html_body, attachment)
    send_email(msg)
