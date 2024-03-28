from datetime import datetime, timedelta
from pyspark.sql import SparkSession

def upload_to_bigquery():
    bucket_name = "de-project-flight-analyzer"
    iata = "NQZ"
    prev_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    columns = [
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
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .getOrCreate()

    df = spark.read.parquet(f"gs://{bucket_name}/{iata}/{prev_date}/*")

    df_columns = df \
        .select(columns)

    df_columns.registerTempTable('temp_table')

    df_result = spark.sql("""
    WITH asd AS (
        SELECT 
            *,
            ROW_NUMBER() OVER (PARTITION BY departure.airport, arrival.airport, departure.scheduled ORDER BY (NULL)) AS rn
        FROM temp_table
    )
    SELECT
        CAST('departure.airport' AS STRING) AS departure.airport,
        CAST('departure.iata' AS STRING) AS departure.iata,
        CAST('departure.timezone' AS TIMESTAMP) AS departure.timezone,
        CAST('departure.scheduled' AS TIMESTAMP) AS departure.scheduled,
        CAST('departure.actual' AS TIMESTAMP) AS departure.actual,
        CAST('departure.delay' AS INTEGER) AS departure.delay,
        CAST('arrival.airport' AS TIMESTAMP) AS arrival.airport,
        CAST('arrival.iata' AS STRING) AS arrival.iata,
        CAST('arrival.timezone' AS TIMESTAMP) AS arrival.timezone,
        CAST('arrival.scheduled' AS TIMESTAMP) AS arrival.scheduled,
        CAST('arrival.actual' AS TIMESTAMP) AS arrival.actual,
        CAST('arrival.delay' AS INTEGER) AS arrival.delay,
        CAST('airline.name' AS STRING) AS airline.name,
        CAST('airline.iata' AS STRING) AS airline.iata,
        CAST('flight.number' AS INTEGER) AS flight.number,
        CAST('flight.iata' AS STRING) AS flight.iata,
    FROM asd
    WHERE rn = 1
    """)
    
    return df_result

#     # Save the data to BigQuery
#     df_result.write.format('bigquery') \
#     .option('table', df_result) \
#     .save()

df_spark = upload_to_bigquery()
print(df_spark)