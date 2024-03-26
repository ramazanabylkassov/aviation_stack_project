from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import requests
import dlt
import os
import gcsfs
           
def upload_to_gcs():
    iata = "NQZ"
    bucket_name = "de-project-flight-analyzer"
    prev_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    fs = gcsfs.GCSFileSystem()

    def fetch_data():
        url_base = f"http://api.aviationstack.com/v1/flights?access_key=4c9daccbaa0ab0dc63923014205e07c3&dep_iata={iata}"
        offset = 0
        while True:
            url = f"{url_base}&offset={offset}"
            response = requests.get(url)
            response.raise_for_status()  # Raises HTTPError for 4xx or 5xx responses
            temp_json = response.json()  # Extract data from JSON
            if not temp_json:
                break
            offset += 100
            yield temp_json

    pipeline = dlt.pipeline(
        pipeline_name='flights_departures',
        destination='filesystem',
        dataset_name=f'{bucket_name}/{iata}'
    )

    load_info = pipeline.run(
        fetch_data(iata), 
        table_name=f"{prev_date}", 
        loader_file_format="parquet",
        write_disposition="append"
        )
    
    return load_info


def print_world():
    return 'World'

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 24),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=10), 
}

# Define the DAG
dag = DAG(
    'first_dag', # DAG ID
    default_args=default_args,
    description='first dag with flight api',
    schedule_interval=timedelta(days=1), # Run once a day
)

task_to_gcs = PythonOperator(
    task_id = "upload_to_gcs",
    python_callable=upload_to_gcs,
    dag=dag
)

task_world = PythonOperator(
    task_id='print_world', # Task ID
    python_callable=print_world,
    dag=dag,
)

# Set the task execution order
task_to_gcs >> task_world # task_hello runs before task_world