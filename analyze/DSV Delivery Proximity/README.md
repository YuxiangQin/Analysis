# DSV Delivery Proximity

## Overview

DSV Delivery Proximity is aimed at identifying opportunities for moving volume to GoLocal network. It provides a mileage breakdown of deliveries from distributor locations, focusing on those within a 0-60 mile radius. This will help in the strategic decision-making.
 
JIRA Ticket [BSCEDA-9169](https://jira.walmart.com/browse/BSCEDA-9169).

## File Description
- `base_table.sql`: Script creating base tables.
- `distributor_addr.sql`: This file contains script pulling unique distributors and their address information.
- `zip_calculation.sql`: Script generating proximity buckets based on zip code calculated distance.
- `geo_calculation.sql`: Script generating proximity buckets based on latitude and longitude calculated distance. **Result shared with stakeholders are created from this script**.
- `Get Geocoding from API.ipynb`: This script is used for getting latitude and longitude from free online api calls, and save results in BigQuery table.

## Installation
Install `dotenv` package, to get environment variables, [official documents](https://pypi.org/project/python-dotenv/).
```bash
pip install python-dotenv  
```
Install `opencage` package, to get api call result from OpenCage, [official documents](https://opencagedata.com/tutorials/geocode-in-python#install).
```bash
pip3 install opencage
```
Install `pandas_gbq`, to save DataFrame to BigQuery, [official documents](https://pandas-gbq.readthedocs.io/en/latest/install.html#pip).
```bash
pip install pandas-gbq
```
You need to install the Google Cloud SDK to import `google.auth` and `google.cloud.bigquery`.  
[Google Cloud SDK Installation Guide](https://cloud.google.com/sdk/docs/install)  
This guide provides detailed instructions on how to install the SDK on various operating systems.
```bash
pip install google-cloud-bigquery
```


## API Key Setup

### positionstack API
Get API Key [here](https://positionstack.com/product).  
More information in [API Documents](https://positionstack.com/documentation).  
Free API call limitation: 25,000 requests/month.

### OpenCage API
Follow instructions in OpenCage [API Documents](https://opencagedata.com/api#authentication) to sign up and get free API Key.  
Free API call limitation: 2,500 requests/day; 1 request/sec.  
**Note:** If you use `opencage` package to call API, seems it automatically handles the 1 request/sec limit. But if you're calling the API using `http` or `requests`, you need to mannually add 1 second gap between API calls. One way to do it as below:  
```python
import time
time.sleep(1) -- 1 second idle time
```

### Maps Data API
The Maps Data API returns the most accurate latitude and longitude information. However, due to the constraints on free API calls, it is recommended to utilize positionstack and OpenCage APIs first, and then supplement with the results from Maps Data API calls.

First, you need to create an account in Rapid API [here](https://rapidapi.com/auth/sign-up?referral=/alexanderxbx/api/maps-data/pricing).  
Second, search for "Maps Data", or follow this [link](https://rapidapi.com/alexanderxbx/api/maps-data/pricing) to subsribe.  
Finally, you can find your API Key under the `Header Parameters` section - `X-RapidAPI-Key` on the [Endpoints](https://rapidapi.com/alexanderxbx/api/maps-data) tab.   
Free API call limitation: 1,000 requests/month.

### Create .env file to store API Keys
Create `.env` file under root directory, save your API Keys following below formatting:  
```
POSITIONSTACK_API_KEY = <your_positionstack_api_key>
CAGE_API_KEY = <your_opencage_api_key>
MAPS_DATA_API_KEY = <your_mapsdata_api_key>
```
Please replace the content inside `< >` with your actual API Key strings.