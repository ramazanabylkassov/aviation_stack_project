from utils import api_to_gcs, gcs_to_bigquery, raw_to_datamart
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

iata='ala'

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
    'FlightsETLAlmaty', 
    default_args=default_args,
    description='flights etl dag',
    schedule_interval=timedelta(days=1), 
)

api_to_gcs = PythonOperator(
    task_id = "api_to_gcs",
    python_callable=api_to_gcs,
    op_kwargs={
        'ds': '{{ ds }}', 
        'iata': f'{iata}'
        },
    dag=dag
)

# gcs_to_bigquery = PythonOperator(
#     task_id = "gcs_to_bigquery",
#     python_callable=gcs_to_bigquery,
#     op_kwargs={
#         'ds': '{{ ds }}', 
#         'iata': f'{iata}'
#         },
#     dag=dag
# )

# raw_to_datamart = PythonOperator(
#     task_id = "raw_to_datamart",
#     python_callable=raw_to_datamart,
#     op_kwargs={
#         'ds': '{{ ds }}', 
#         'iata': f'{iata}'
#         },
#     dag=dag
# )

api_to_gcs 
# >> gcs_to_bigquery
#  >> raw_to_datamart