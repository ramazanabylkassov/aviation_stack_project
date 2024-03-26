from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import requests
import dlt
import os

def first_dag():
    bucket_name = "de-project-flight-analyzer"
    # Set the bucket_url. We can also use a local folder
    os.environ['DESTINATION__FILESYSTEM__BUCKET_URL'] = f'gs://{bucket_name}'

    url = "https://api.aviationstack.com/v1/flights \
        ? access_key = b1bb7ec89c84dee6f117d808df009e49 \
            & flight_date = 2024-03-25 \
                & dep_iata = NQZ"

    # Define your pipeline
    pipeline = dlt.pipeline(
        pipeline_name='my_pipeline',
        destination='filesystem',
        dataset_name='test_flight_data'
    )

    def download_jsonl(url):
        response = requests.get(url)
        response.raise_for_status()  # Raise an HTTPError for bad responses
        return response.json()

    # Run the pipeline with the generator we created earlier.
    load_info = pipeline.run(download_jsonl(url), table_name="users", loader_file_format="parquet")
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
    python_callable=first_dag,
    dag=dag
)

task_world = PythonOperator(
    task_id='print_world', # Task ID
    python_callable=print_world,
    dag=dag,
)

# Set the task execution order
task_to_gcs >> task_world # task_hello runs before task_world