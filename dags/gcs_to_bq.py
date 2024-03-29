from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from pyspark.sql import SparkSession

def print_hello():
    return 'Hello'

def upload_to_bigquery():
    bucket_name = "de-project-flight-analyzer"
    iata = "NQZ"
    prev_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    columns = [
        'flight_date',
        'departure.airport',
        'departure.iata',
        'departure.timezone',
        'departure.scheduled',
        'departure.actual',
        'departure.delay',
        'arrival.airport',
        'arrival.iata',
        'arrival.timezone',
        'arrival.scheduled',
        'arrival.actual',
        'arrival.delay',
        'airline.name',
        'airline.iata',
        'flight.number',
        'flight.iata',
    ]
    
    spark = SparkSession.builder \
        .appName("GCS Access") \
        .config("spark.jars.packages", "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.5") \
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
        .getOrCreate()

    df = spark.read.parquet(f"gs://{bucket_name}/{iata}/{prev_date}/*")

    df_columns = df \
        .select(columns)

    df_columns.registerTempTable('temp_table')

    df_result = spark.sql(f"""
    WITH asd AS (
        SELECT 
            *,
            ROW_NUMBER() OVER (PARTITION BY departure_airport, arrival_airport, departure_scheduled ORDER BY (NULL)) AS rn
        FROM temp_table
    )
    SELECT
        CAST(departure.airport AS STRING) AS departure_airport,
        CAST(departure.iata AS STRING) AS departure_iata,
        CAST(departure.timezone AS TIMESTAMP) AS departure_timezone,
        CAST(departure.scheduled AS TIMESTAMP) AS departure_scheduled,
        CAST(departure.actual AS TIMESTAMP) AS departure_actual,
        CAST(departure.delay AS INTEGER) AS departure_delay,
        CAST(arrival.airport AS STRING) AS arrival_airport,
        CAST(arrival.iata AS STRING) AS arrival_iata,
        CAST(arrival.timezone AS TIMESTAMP) AS arrival_timezone,
        CAST(arrival.scheduled AS TIMESTAMP) AS arrival_scheduled,
        CAST(arrival.actual AS TIMESTAMP) AS arrival_actual,
        CAST(arrival.delay AS INTEGER) AS arrival_delay,
        CAST(airline.name AS STRING) AS airline_name,
        CAST(airline.iata AS STRING) AS airline_iata,
        CAST(flight.number AS STRING) AS flight_number,
        CAST(flight.iata AS STRING) AS flight_iata
    FROM asd
    WHERE rn = 1 AND flight_date = '{prev_date}'
    """)

    # Save the data to BigQuery
    df_result.write.format('bigquery') \
    .option('table', f'de-project-flight-analyzer.cities_raw_data.{iata}') \
    .save()

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
    'second_dag', 
    default_args=default_args,
    description='second dag with flight api',
    schedule_interval=timedelta(days=1), 
)

task_hello = PythonOperator(
    task_id='print_hello', 
    python_callable=print_hello,
    dag=dag,
)

task_to_bigquery = PythonOperator(
    task_id = "upload_to_bigquery",
    python_callable=upload_to_bigquery,
    dag=dag
)

task_world = PythonOperator(
    task_id='print_world', 
    python_callable=print_world,
    dag=dag,
)

task_hello >> task_to_bigquery >> task_world 