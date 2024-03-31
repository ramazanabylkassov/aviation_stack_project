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
    json_file_path = f'{iata}/{iata}_{yesterday}/*'

    # Initialize a Google Cloud Storage client
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    blob = bucket.blob(json_file_path)
    json_data = json.loads(blob.download_as_text())
    
    columns = [
        'departure.airport',
        'departure.iata',
        'departure.timezone',
        'departure.scheduled',
        'departure.actual',
        'departure.delay',
        'arrival.airport',
        'arrival.iata',
        'arrival.timezone',
        'arrival.scheduled',
        'arrival.actual',
        'arrival.delay',
        'airline.name',
        'airline.iata',
        'flight.number',
        'flight.iata',
    ]

    temp_df = pd.json_normalize(json_data)
    df = temp_df[columns]
    print(df)

def raw_to_datamart(ds=None, iata=None):
    print('raw_to_datamart')