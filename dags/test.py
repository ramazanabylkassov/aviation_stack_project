from utils import raw_to_datamart, gcs_to_bigquery, api_to_gcs
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

# cit_api_to_gcs = PythonOperator(
#     task_id = f"cit_api_to_gcs",
#     python_callable=api_to_gcs,
#     op_kwargs={
#         'ds': '{{ ds }}', 
#         'iata': f'cit'
#         },
#     dag=dag
# )

cit_gcs_to_bigquery = PythonOperator(
    task_id = f"cit_gcs_to_bigquery",
    python_callable=gcs_to_bigquery,
    op_kwargs={
        'ds': '{{ ds }}', 
        'iata': 'cit'
        },
    dag=dag
)

cities = {'ASTANA': 'nqz', 'ALMATY': 'ala', 'SHYMKENT': 'cit'}

raw_to_datamart = PythonOperator(
    task_id = "BIGQUERY_raw_to_datamart",
    python_callable=raw_to_datamart,
    op_kwargs={
        'ds': '{{ ds }}', 
        'cities': cities
        },
    dag=dag
)

cit_gcs_to_bigquery >> raw_to_datamart

# Update 45