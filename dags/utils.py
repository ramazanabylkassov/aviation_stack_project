import gzip
import io
import os
import requests
import dlt
from datetime import datetime, timedelta
from google.cloud import storage
import pandas as pd
import json
from google.cloud import bigquery
from google.api_core.exceptions import NotFound, GoogleAPIError
import numpy as np

os.environ['FLIGHTS_DEPARTURES__DESTINATION__FILESYSTEM__BUCKET_URL'] = f'gs://de-project-flight-analyzer'

def fetch_csv(iata=None):

    API_ACCESS_KEY = os.environ.get(f'API_{iata}_ACCESS_KEY')
    if not API_ACCESS_KEY:
        raise ValueError(f'API_{iata}_ACCESS_KEY not defined')
    url_base = f"http://api.aviationstack.com/v1/flights?access_key={API_ACCESS_KEY}&dep_iata={iata.upper()}"
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

def transform_data(json_data=None, yesterday=None, unique_key=None):
    df = pd.json_normalize(json_data)
    yesterday = yesterday.strftime('%Y-%m-%d')
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
    
    # Select the desired columns first
    df_old = df[old_columns].copy()
    # Apply the filter for 'yesterday' on the 'departure__scheduled' column
    df_filtered = df_old[df_old['flight_date'] == yesterday]
    # Rename columns by replacing double underscores with single underscores
    df_filtered.columns = new_columns
    df_filtered = df_filtered.replace({np.nan: None}).drop_duplicates(subset=unique_key)
    # Convert the filtered and renamed DataFrame to a dictionary
    json_file = df_filtered.to_dict(orient='records')  # Assuming you want a list of records
    
    return json_file, new_columns

    # for json_line in json_file:
    #     # Convert 'flight_number' to int, handling None correctly
    #     flight_number = json_line.get('flight_number')
    #     json_line['flight_number'] = int(flight_number) if flight_number is not None else None

    #     # Convert 'departure_delay' to float, handling None correctly
    #     departure_delay = json_line.get('departure_delay')
    #     json_line['departure_delay'] = float(departure_delay) if departure_delay is not None else None

    #     # Convert 'arrival_delay' to float, handling None correctly
    #     arrival_delay = json_line.get('arrival_delay')
    #     json_line['arrival_delay'] = float(arrival_delay) if arrival_delay is not None else None

    #     yield json_line

def load_json_to_temp_table(json_data, dataset_id, temp_table_id, schema, location="US"):
    client = bigquery.Client()

    table_full_id = f"{dataset_id}.{temp_table_id}"
    job_config = bigquery.LoadJobConfig(schema=schema)

    # Create TableReference object
    table_ref = bigquery.TableReference.from_string(table_full_id)

    try:
        # Try to fetch the table to see if it exists
        client.get_table(table_ref)
        print(f"Table {temp_table_id} already exists.")
    except NotFound:
        # If the table does not exist, simply proceed as the table will be created during load
        print(f"Table {temp_table_id} does not exist, will be created during data load.")
    except GoogleAPIError as e:
        print(f"Encountered an error checking for table existence: {e}")
        return

    try:
        # Perform the data load
        load_job = client.load_table_from_json(
            json_data,
            destination=table_ref,
            job_config=job_config
        )
        # Wait for the job to complete
        load_job.result()
        print(f"Data loaded into temporary table {temp_table_id}.")
    except GoogleAPIError as e:
        print(f"Failed to load data into {temp_table_id}: {e}")

def merge_temp_table_into_main_table(dataset_id, temp_table_id, main_table_id, unique_key_columns, all_columns):
    client = bigquery.Client()

    # Create the ON clause for the composite key
    on_clause = ' AND '.join([f"T.{col} = S.{col}" for col in unique_key_columns])

    # Dynamically create the SET clause for all other columns
    set_clause = ', '.join([f"T.{col} = S.{col}" for col in all_columns if col not in unique_key_columns])

    merge_sql = f"""
    MERGE `{dataset_id}.{main_table_id}` T
    USING `{dataset_id}.{temp_table_id}` S
    ON {on_clause}
    WHEN MATCHED THEN
        UPDATE SET {set_clause}
    WHEN NOT MATCHED THEN
        INSERT ROW;
    """

    # Execute the MERGE query
    query_job = client.query(merge_sql)
    query_job.result()  # Wait for the query to finish
    print(f"Merge completed. Temporary data merged into {main_table_id}.")

def gcs_to_bigquery(ds=None, iata=None):
    # Define your GCS parameters
    ds_minus_one = datetime.strptime(ds, '%Y-%m-%d') - timedelta(days=1)
    yesterday = ds_minus_one.strftime('%Y_%m_%d')
    bucket_name = 'de-project-flight-analyzer'
    json_file_path = f'{iata}/{iata}_{yesterday}/'
    unique_key_columns = ["departure_scheduled", "arrival_airport"]  # Adjust to match your schema's unique identifier

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
    
    json_to_bq, all_columns = transform_data(json_data=all_data, yesterday=ds_minus_one, unique_key=unique_key_columns)

    # Define the schema as before
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

    bigquery_client = bigquery.Client()
    dataset_id = f'{bucket_name}.cities_raw_data'
    main_table_id = f'{iata}'
    full_table_id = f"{dataset_id}.{main_table_id}"
    # # Try to fetch the table, and create it if it doesn't exist
    try:
        bigquery_client.get_table(full_table_id)  # This checks if the table exists
        print(f"Table {full_table_id} already exists.")
    except NotFound:
        table = bigquery.Table(full_table_id, schema=schema)
        bigquery_client.create_table(table)  # This creates the table
        print(f"Table {full_table_id} created.")

    
    # errors = bigquery_client.insert_rows_json(f"{dataset_id}.{table_id}", json_to_bq)
    # if errors == []:
    #     print("New rows have been added.")
    # else:
    #     print("Encountered errors while inserting rows: {}".format(errors))

    # Load JSON data into the temporary table
    temp_table_id = 'temp_table_for_merging'
    load_json_to_temp_table(json_to_bq, dataset_id, temp_table_id, schema)

    # Merge the temporary table into the main table
    merge_temp_table_into_main_table(dataset_id, temp_table_id, main_table_id, unique_key_columns, all_columns)

def raw_to_datamart(ds=None, iata=None):
    # Initialize a BigQuery client
    client = bigquery.Client()

    # Define your SQL query for data transformation
    # This is a simple example that creates a new table with transformed data
    # Replace this with your actual data transformation query
    query = """
        CREATE OR REPLACE TABLE `project.dataset.new_table` AS
        SELECT 
            column1, 
            column2,
            column1 * column2 AS column3  # An example transformation
        FROM 
            `project.dataset.original_table`
    """

    # Run the query
    query_job = client.query(query)

    # Wait for the query to finish
    query_job.result()

    print("Query completed. The data has been transformed and stored in project.dataset.new_table.")
    