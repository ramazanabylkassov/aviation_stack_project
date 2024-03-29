import requests
import dlt
import gcsfs
import os
import csv

def fetch_csv(iata=None):
    API_NQZ_ACCESS_KEY = os.environ.get('API_NQZ_ACCESS_KEY')
    if not API_NQZ_ACCESS_KEY:
        raise 'API_NQZ_ACCESS_KEY not defined'
    url_base = f"http://api.aviationstack.com/v1/flights?access_key={API_NQZ_ACCESS_KEY}&dep_iata={iata}"
    offset = 0
    output_json = []

    while True:
        url = f"{url_base}&offset={offset}"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        temp_json = data.get('data', [])
        output_json.extend(temp_json)
        if int(data["pagination"]["count"]) < 100:
            break
        offset += 100
    
    def convert_to_csv(json_data=None):
        csv_file = "output.csv"
        with open(csv_file, 'w', newline='') as file:
            csv_writer = csv.writer(file)
            header = json_data[0].keys()
            csv_writer.writerow(header)
            for item in json_data:
                csv_writer.writerow(item.values())
        return csv_file
    
    return convert_to_csv(output_json)

def upload_to_gcs(ds=None, iata=None):
    
    print(f"The execution date is: {ds}")

    bucket_name = "de-project-flight-analyzer"
    fs = gcsfs.GCSFileSystem()
    FLIGHTS_DEPARTURES__DESTINATION__FILESYSTEM__BUCKET_URL = f'gs://{bucket_name}'

    pipeline = dlt.pipeline(
        pipeline_name='flights_departures',
        destination='filesystem',
        dataset_name=f'{iata}'
    )

    load_info = pipeline.run(
        fetch_csv(iata=iata), 
        table_name=f"{ds}", 
        loader_file_format="csv",
        write_disposition="replace"
        )
    
    print(load_info)

