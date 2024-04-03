import gzip
import io
import os
import requests
from datetime import datetime, timedelta
from google.cloud import storage
import pandas as pd
import json
from google.cloud import bigquery
from google.api_core.exceptions import NotFound, GoogleAPIError
import numpy as np
import dlt

project_name = "de-project-flight-analyzer"

def api_to_gcs(ds=None, iata=None):
    start_time = datetime.now()
    print(f"TASK 1: API -> GCS for {iata.upper()} STARTED")

    os.environ[f'FLIGHTS_DEPARTURES_{iata.upper()}__DESTINATION__BUCKET_URL'] = f'gs://{project_name}'
    yesterday = (datetime.strptime(ds, '%Y-%m-%d')).strftime('%Y_%m_%d')
    table_name = f"{iata}_{yesterday}"

    def fetch_csv(iata=None):
        API_ACCESS_KEY = os.environ.get(f'API_{iata}_ACCESS_KEY')
        if not API_ACCESS_KEY:
            raise ValueError(f'API_{iata}_ACCESS_KEY not defined')
        url_base = f"http://api.aviationstack.com/v1/flights?access_key={API_ACCESS_KEY}&dep_iata={iata.upper()}"
        offset = 0
        output_file = []
        api_total_size = 0

        while True:
            url = f"{url_base}&offset={offset}"
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            api_total_size = int(data["pagination"]["total"])
            output_file.extend(data.get('data', []))
            if int(data["pagination"]["count"]) < 100:
                break
            offset += 100
        
        return output_file, api_total_size

    json_file, api_total_size = fetch_csv(iata=iata)

    pipeline = dlt.pipeline(
        pipeline_name=f'flights_departures_{iata}',
        destination='filesystem',
        dataset_name=f'{iata}'
    )
    
    if json_file:
        load_info = pipeline.run(
            json_file, 
            table_name=table_name, 
            write_disposition="replace"
            )
        print(f"""dlt load info:
              {load_info}""")
    else:
        print("No data to upload.")
    
    end_time = datetime.now()
    time_taken = end_time - start_time
    
    # Print report on the task
    print(f"""
          TASK 1: API -> GCS for {iata.upper()} FINITO
          RESULTS:
            - Logical date: {ds}
            - Start time: {start_time}
            - End time: {end_time}
            - Time taken: {time_taken}
            - GCS path: {project_name}/{iata}/{table_name}
            - API request size: {api_total_size}
            """)

def gcs_to_bigquery(ds=None, iata=None):
    start_time = datetime.now()
    print(f"TASK 2: GCS -> BQ for {iata.upper()} STARTED")

    print(f'ds: {ds}')
    print(f'ds type: {type(ds)}')
    
    # Set connections to GCS and BQ
    gcs_client = storage.Client()
    bq_client = bigquery.Client()

    # Define GCS parameters
    ds_datetime = datetime.strptime(ds, '%Y-%m-%d')
    yesterday_underscore = ds_datetime.strftime('%Y_%m_%d')
    yesterday_dash = ds_datetime.strftime('%Y-%m-%d')
    table_name = f"{iata}_{yesterday_underscore}"
    json_file_path = f'{iata}/{table_name}/'
    unique_key_columns = ["departure_scheduled", "arrival_airport"]

    # Get the json data from GCS bucket
    bucket = gcs_client.bucket(project_name)
    blobs = bucket.list_blobs(prefix=json_file_path)
    json_data = []
    for blob in blobs:
        bytes_data = blob.download_as_bytes()
        with gzip.open(io.BytesIO(bytes_data), 'rt', encoding='utf-8') as gzip_file:
            for line in gzip_file:
                data = json.loads(line)
                json_data.append(data)
        break
    else:
        raise FileNotFoundError(f"No files found for prefix {json_file_path}")
    
    def transform_data(json_data=None, yesterday=None, unique_key=None):
        # Define parameters
        old_columns = [
            'flight_date',
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
        new_columns = [column.replace('__', '_') for column in old_columns]
        
        # Transform the data via pandas
        df = pd.json_normalize(json_data)[old_columns].copy()
        df_filtered = df[df['flight_date'] == yesterday]
        df_filtered.columns = new_columns
        df_filtered = df_filtered.replace({np.nan: None}).drop_duplicates(subset=unique_key, keep='last')
        
        # Convert back to json
        json_file = df_filtered.to_dict(orient='records')
        
        return json_file, new_columns

    json_to_bq, new_columns = transform_data(
        json_data=json_data, 
        yesterday=yesterday_dash, 
        unique_key=unique_key_columns
        )

    # Define the schema
    schema = [
        bigquery.SchemaField("flight_date", "DATE"),
        bigquery.SchemaField("flight_number", "INT64"),
        bigquery.SchemaField("flight_iata", "STRING"),
        bigquery.SchemaField("departure_airport", "STRING"),
        bigquery.SchemaField("departure_iata", "STRING"),
        bigquery.SchemaField("departure_scheduled", "TIMESTAMP"),
        bigquery.SchemaField("departure_actual", "TIMESTAMP"),
        bigquery.SchemaField("departure_delay", "FLOAT64"),
        bigquery.SchemaField("arrival_airport", "STRING"),
        bigquery.SchemaField("arrival_iata", "STRING"),
        bigquery.SchemaField("arrival_timezone", "STRING"),
        bigquery.SchemaField("arrival_scheduled", "TIMESTAMP"),
        bigquery.SchemaField("arrival_actual", "TIMESTAMP"),
        bigquery.SchemaField("arrival_delay", "FLOAT64"),
        bigquery.SchemaField("airline_name", "STRING"),
        bigquery.SchemaField("airline_iata", "STRING")
    ]

    dataset_id = f'{project_name}.flights_raw_data'
    main_table_id = f'{iata}'
    main_table_full_id = f"{dataset_id}.{main_table_id}"
    temp_table_id = f'temp_table_{iata}'
    temp_table_full_id = f"{dataset_id}.{temp_table_id}"

    try:
        bq_client.get_table(main_table_full_id)
        print(f"Table {main_table_full_id} already exists.")
    except NotFound:
        table = bigquery.Table(main_table_full_id, schema=schema)
        bq_client.create_table(table)
        print(f"Table {main_table_full_id} created.")
    

    def load_json_to_temp_table(json_data, temp_table_full_id, schema):
        job_config = bigquery.LoadJobConfig(schema=schema)
        table_ref = bigquery.TableReference.from_string(temp_table_full_id)

        try:
            bq_client.get_table(table_ref)
            print(f"Table {temp_table_id} already exists.")
        except NotFound:
            print(f"Table {temp_table_id} does not exist, will be created during data load.")
        except GoogleAPIError as e:
            print(f"Encountered an error checking for table existence: {e}")
            return

        try:
            load_job = bq_client.load_table_from_json(
                json_data,
                destination=table_ref,
                job_config=job_config
            )
            load_job.result()
            print(f"Data loaded into temporary table {temp_table_id}.")
        except GoogleAPIError as e:
            print(f"Failed to load data into {temp_table_id}: {e}")

    load_json_to_temp_table(json_to_bq, temp_table_full_id, schema)

    # Merge the temporary table into the main table
    def merge_temp_table_into_main_table(main_table_full_id, temp_table_full_id, unique_key_columns, new_columns):
        # Merge temporary and main tables
        compare_clause = ' AND '.join([f"T.{col} = S.{col}" for col in unique_key_columns])
        update_clause = ', '.join([f"T.{col} = S.{col}" for col in new_columns if col not in unique_key_columns])
        merge_sql = f"""
        MERGE `{main_table_full_id}` T
        USING `{temp_table_full_id}` S
        ON {compare_clause}
        WHEN MATCHED THEN
            UPDATE SET {update_clause}
        WHEN NOT MATCHED THEN
            INSERT ROW
        """
        try:
            query_job = bq_client.query(merge_sql)
            query_job.result()
            print(f"Merge completed. Temporary data merged into {main_table_id}.")
        except Exception as e:
            # Catching any type of exception and printing a generic error message
            print("An error occurred during the merge operation:", e)
        
        # Delete the temporary table
        clear_temp_table_sql = f"DROP TABLE `{dataset_id}.{temp_table_id}`"
        clear_job = bq_client.query(clear_temp_table_sql)
        clear_job.result()

    merge_temp_table_into_main_table(main_table_full_id, temp_table_full_id, unique_key_columns, new_columns)

    end_time = datetime.now()
    time_taken = end_time - start_time
    
    # Print report on the task
    print(f"""
          TASK 2: GCS -> BQ for {iata.upper()} FINITO
          RESULTS:
            - Logical date: {ds}
            - Start time: {start_time}
            - End time: {end_time}
            - Time taken: {time_taken}
            - BQ path of the result table: {main_table_full_id}
            """)

def raw_to_datamart(ds=None, cities=None):
    start_time = datetime.now()
    print(f"TASK 3: BQ(raw data: {', '.join(cities.keys())}) -> BQ(data mart) STARTED")
    
    # Initialize BigQuery client
    bq_client = bigquery.Client()

    # Define datasets and tables
    source_dataset_id = "flights_raw_data"
    destination_dataset_id = "flights_datamart"
    destination_table_id = "total_flights_data"
    full_table_id = f"{destination_dataset_id}.{destination_table_id}"

    kaz_iata_str = "'SCO', 'AKX', 'SAH', 'ALA', 'AYK', 'ATX', 'GUW', 'BXH', 'EKB', 'KGF', 'KOV', 'KSN', 'KZO', 'NQZ', 'URA', 'UKK', 'PWQ', 'PPK', 'PLX', 'CIT', 'TDK', 'DMB', 'HSA', 'UZR', 'USJ', 'SZI', 'DZN'"

    # Query data from source tables
    combined_data = pd.concat([bq_client.query(f"""
                                            SELECT 
                                                flight_date,
                                                departure_airport,
                                                departure_scheduled,
                                                departure_actual,
                                                COALESCE(departure_delay, 0) AS departure_delay,
                                                arrival_airport,
                                                arrival_iata,
                                                airline_name,
                                                FORMAT_DATE('%A', flight_date) AS weekday, 
                                                EXTRACT(HOUR FROM departure_scheduled) AS hour,
                                                IF(arrival_iata IN ({kaz_iata_str}), 'Kazakhstan', 'International') AS flight_destination_type
                                            FROM `{source_dataset_id}.{table_id}`
                                            """).to_dataframe() for table_id in list(cities.values())])

    # Create or replace table in destination dataset
    destination_table_ref = bq_client.dataset(destination_dataset_id).table(destination_table_id)
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")  # Replace the table if it exists
    job = bq_client.load_table_from_dataframe(combined_data, destination_table_ref, job_config=job_config)
    job.result()

    print(f"Table {destination_dataset_id}.{destination_table_id} created or replaced with data from source tables.")

    end_time = datetime.now()
    time_taken = end_time - start_time
    
    # Print report on the task
    print(f"""
          TASK 3: BQ(raw data: {', '.join(cities.keys())}) -> BQ(data mart) FINITO
          RESULTS:
            - Logical date: {ds}
            - Start time: {start_time}
            - End time: {end_time}
            - Time taken: {time_taken}
            - BQ path of the result table: {full_table_id}
            """)
