from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import requests
import dlt
import gcsfs
import os
import json
           
def upload_to_gcs():
    iata = "NQZ"
    bucket_name = "de-project-flight-analyzer"
    prev_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    os.environ['FLIGHTS_DEPARTURES__DESTINATION__FILESYSTEM__BUCKET_URL'] = f'gs://{bucket_name}'
    fs = gcsfs.GCSFileSystem()

    def fetch_data():
        url_base = f"http://api.aviationstack.com/v1/flights?access_key=e731e10ffd59f5dee0f69b8c26460607&dep_iata={iata}"
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
        
        return json.dumps(output_json)

    pipeline = dlt.pipeline(
        pipeline_name='flights_departures',
        destination='filesystem',
        dataset_name=f'{iata}'
    )

    load_info = pipeline.run(
        fetch_data(), 
        table_name=f"{prev_date}", 
        loader_file_format="parquet",
        write_disposition="append"
        )
    
    return load_info

def print_world():
    return 'World'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 24),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=10), 
}

dag = DAG(
    'first_dag', 
    default_args=default_args,
    description='first dag with flight api',
    schedule_interval=timedelta(days=1), 
)

task_to_gcs = PythonOperator(
    task_id = "upload_to_gcs",
    python_callable=upload_to_gcs,
    dag=dag
)

task_world = PythonOperator(
    task_id='print_world', 
    python_callable=print_world,
    dag=dag,
)

task_to_gcs >> task_world 

# here is an update