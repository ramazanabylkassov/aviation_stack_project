from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# Define the Python function for each task
def print_hello():
    return 'Hello'

def test_print_hello():
    """Test that the print_hello function returns 'Hello'."""
    assert print_hello() == 'Hello', "print_hello should return 'Hello'"

def print_world():
    return 'World'

def test_print_world():
    """Test that the print_world function returns 'World'."""
    assert print_world() == 'World', "print_world should return 'World'"

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'hello_world_dag', # DAG ID
    default_args=default_args,
    description='A simple Hello World DAG',
    schedule_interval=timedelta(days=1), # Run once a day
)

# Define the tasks
task_hello = PythonOperator(
    task_id='print_hello', # Task ID
    python_callable=print_hello,
    dag=dag,
)

task_world = PythonOperator(
    task_id='print_world', # Task ID
    python_callable=print_world,
    dag=dag,
)

# Set the task execution order
task_hello >> task_world # task_hello runs before task_world
