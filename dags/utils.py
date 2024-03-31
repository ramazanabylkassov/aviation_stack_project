import os
import requests
import dlt
from datetime import datetime, timedelta
from google.cloud import storage
import pandas as pd
import json

os.environ['FLIGHTS_DEPARTURES__DESTINATION__FILESYSTEM__BUCKET_URL'] = f'gs://de-project-flight-analyzer'

def fetch_csv(iata=None):
    API_NQZ_ACCESS_KEY = os.environ.get('API_NQZ_ACCESS_KEY')
    if not API_NQZ_ACCESS_KEY:
        raise ValueError('API_NQZ_ACCESS_KEY not defined')
    url_base = f"http://api.aviationstack.com/v1/flights?access_key={API_NQZ_ACCESS_KEY}&dep_iata={iata}"
    offset = 0
    output_file = []

    while True:
        url = f"{url_base}&offset={offset}"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        temp_json = data.get('data', [])
        output_file.extend(temp_json)
        if int(data["pagination"]["count"]) < 100:
            break
        offset += 100
    return output_file

def api_to_gcs(ds=None, iata=None):
    ds_datetime = datetime.strptime(ds, '%Y-%m-%d')
    yesterday = (ds_datetime - timedelta(days=1)).strftime('%Y_%m_%d')
    pipeline = dlt.pipeline(
        pipeline_name='flights_departures',
        destination='filesystem',
        dataset_name=f'{iata}'
    )
    json_file = fetch_csv(iata=iata)
    if json_file:
        load_info = pipeline.run(
            json_file, 
            table_name=f"{iata}_{yesterday}", 
            write_disposition="replace"
            )
        print(load_info)
    else:
        print("No data to upload.")

def gcs_to_bigquery(ds=None, iata=None):
    # Define your GCS parameters
    ds_datetime = datetime.strptime(ds, '%Y-%m-%d')
    yesterday = (ds_datetime - timedelta(days=1)).strftime('%Y_%m_%d')
    bucket_name = 'de-project-flight-analyzer'
    json_file_path = f'{iata}/{iata}_{yesterday}/'

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=json_file_path)
    for blob in blobs:
        json_string = blob.download_as_text()
        json_data = json.loads(json_string)
        break
    else:  # No files found
        raise FileNotFoundError(f"No files found for prefix {json_file_path}")
    
    # Specify data types and parse_dates
    flight_dtypes = {
        'departure.airport': str,
        'departure.iata': str,
        'departure.timezone': str,
        'departure.delay': 'Int64',
        'arrival.airport': str,
        'arrival.iata': str,
        'arrival.timezone': str,
        'arrival.delay': 'Int64',
        'airline.name': str,
        'airline.iata': str,
        'flight.number': 'Int64',
        'flight.iata': str
    }
    parse_dates = ['departure.scheduled', 'arrival.scheduled', 'departure.actual', 'arrival.actual']

    df = pd.json_normalize(json_data)

    # Adjust data types
    for column, dtype in flight_dtypes.items():
        df[column] = df[column].astype(dtype)

    # Parse dates
    for date_column in parse_dates:
        df[date_column] = pd.to_datetime(df[date_column])

    print(df)

def raw_to_datamart(ds=None, iata=None):
    print('raw_to_datamart')