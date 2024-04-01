from utils import gcs_to_bigquery
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 30),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=10), 
}

dag = DAG(
    'testdag', 
    default_args=default_args,
    description='flights etl dag',
    schedule_interval=timedelta(days=1), 
)

raw_to_datamart = PythonOperator(
    task_id = "raw_to_datamart",
    python_callable=raw_to_datamart,
    op_kwargs={
        'ds': '{{ ds }}', 
        'iata': 'nqz'
        },
    dag=dag
)

raw_to_datamart 

# Update 33