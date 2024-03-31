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
    df = pd.json_normalize(json_data)

    old_columns = [
        'flight__number',
        'flight__iata',
        'departure__airport',
        'departure__iata',
        'departure__scheduled',
        'departure__actual',
        'departure__delay',
        'arrival__airport',
        'arrival__iata',
        'arrival__timezone',
        'arrival__scheduled',  
        'arrival__actual',
        'arrival__delay',
        'airline__name',
        'airline__iata',
    ]
    df = df[old_columns]
    df.columns = [column.replace('__', '_') for column in old_columns]

    print(df.drop_duplicates())

    return df.drop_duplicates().to_json()

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
        with gzip.open(io.BytesIO(bytes_data), 'rt', encoding='utf-8') as gzip_file:
            for line in gzip_file:
                data = json.loads(line)
                # Perform your data transformation here
                all_data.append(data)
        break
    else:  # No files found
        raise FileNotFoundError(f"No files found for prefix {json_file_path}")
    
    json_file = transform_data(json_data=all_data)

    print(json_file)
    
    # Define your pipeline
    pipeline = dlt.pipeline(
        pipeline_name='upload_to_bigquery',
        destination='bigquery',
        dataset_name='cities_raw_data'
    )

    if json_file:
        load_info = pipeline.run(
            json_file, 
            table_name=f'{iata}',
            write_disposition="append",
            )
        print(load_info)
    else:
        print("No data to upload.")

def raw_to_datamart(ds=None, iata=None):
    print('raw_to_datamart')