import os
import requests
import dlt  # Assuming you have a library named dlt for handling the pipeline
from datetime import datetime, timedelta
import pandas as pd

def convert_to_csv(json_data):
    if not json_data:
        return None  # Handling the case where there is no data
    df = pd.json_normalize(json_data)
    return df.to_csv('output.csv', index=False)

def fetch_csv(iata=None):
    API_NQZ_ACCESS_KEY = os.environ.get('API_NQZ_ACCESS_KEY')
    if not API_NQZ_ACCESS_KEY:
        raise ValueError('API_NQZ_ACCESS_KEY not defined')  # Corrected raise statement
    url_base = f"http://api.aviationstack.com/v1/flights?access_key={API_NQZ_ACCESS_KEY}&dep_iata={iata}"
    offset = 0
    output_json = []

    while True:
        url = f"{url_base}&offset={offset}"
        response = requests.get(url)
        response.raise_for_status()  # Good to have a try-except around this
        data = response.json()
        temp_json = data.get('data', [])
        output_json.extend(temp_json)
        if int(data["pagination"]["count"]) < 100:
            break
        offset += 100

    return convert_to_csv(output_json)

def upload_to_gcs(ds=None, iata=None):
    ds_datetime = datetime.strptime(ds, '%Y-%m-%d')
    yesterday = (ds_datetime - timedelta(days=1)).strftime('%Y-%m-%d')

    print(f"The execution date is: {yesterday}")

    bucket_name = "de-project-flight-analyzer"
    # Setting the environment variable at the start of your script/program is usually better
    os.environ['FLIGHTS_DEPARTURES__DESTINATION__FILESYSTEM__BUCKET_URL'] = f'gs://{bucket_name}'

    # Ensure your pipeline and destination configuration correctly utilizes the GCS bucket
    pipeline = dlt.pipeline(
        pipeline_name='flights_departures',
        destination='filesystem',  # Confirm this is correct for GCS
        dataset_name=f'{iata}'
    )

    csv_file = fetch_csv(iata=iata)
    if csv_file:
        load_info = pipeline.run(
            csv_file, 
            table_name=f"{yesterday}", 
            loader_file_format="csv",
            write_disposition="replace"
            )
        print(load_info)
    else:
        print("No data to upload.")

# Assuming you need to call upload_to_gcs somewhere here
