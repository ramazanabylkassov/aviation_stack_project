import gzip
import io
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

def transform_data(json_data=None):
    # Specify data types and parse_dates
    flight_dtypes = {
        'departure_airport': str,
        'departure_iata': str,
        'departure_timezone': str,
        'departure_delay': 'Int64',
        'arrival_airport': str,
        'arrival_iata': str,
        'arrival_timezone': str,
        'arrival_delay': 'Int64',
        'airline_name': str,
        'airline_iata': str,
        'flight_number': 'Int64',
        'flight_iata': str
    }
    parse_dates = ['departure_scheduled', 'arrival_scheduled', 'departure_actual', 'arrival_actual']

    df = pd.json_normalize(json_data)

    for column in df.columns:
        print(column)

    # Adjust data types
    for column, dtype in flight_dtypes.items():
        df[column] = df[column].astype(dtype)

    # Parse dates
    for date_column in parse_dates:
        df[date_column] = pd.to_datetime(df[date_column])

    return df[list(flight_dtypes.keys())].drop_duplicates()


def gcs_to_bigquery(ds=None, iata=None):
    # Define your GCS parameters
    ds_datetime = datetime.strptime(ds, '%Y-%m-%d')
    yesterday = (ds_datetime - timedelta(days=1)).strftime('%Y_%m_%d')
    bucket_name = 'de-project-flight-analyzer'
    json_file_path = f'{iata}/{iata}_{yesterday}/'

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=json_file_path)

    all_data = []

    for blob in blobs:
                # Download the blob as bytes
        bytes_data = blob.download_as_bytes()

        # Check if the file is gzip-compressed
        if bytes_data[:2] == b'\x1f\x8b':  # gzip signature
            # Use gzip to decompress
            with gzip.open(io.BytesIO(bytes_data), 'rt', encoding='utf-8') as gzip_file:
                for line in gzip_file:
                    data = json.loads(line)
                    # Perform your data transformation here
                    all_data.append(data)
        else:
            # If not compressed, process normally as UTF-8 text
            text_data = bytes_data.decode('utf-8')
            for line in text_data.splitlines():
                data = json.loads(line)
                # Perform your data transformation here
                all_data.append(data)
        break
    else:  # No files found
        raise FileNotFoundError(f"No files found for prefix {json_file_path}")
    
    df_filtered = transform_data(json_data=all_data)
    print(df_filtered)

def raw_to_datamart(ds=None, iata=None):
    print('raw_to_datamart')