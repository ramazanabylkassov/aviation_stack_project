# test_dag_integrity.py

import pytest
from airflow.models import DagBag, TaskInstance
from datetime import datetime

@pytest.fixture(scope="session")
def dag_bag():
    return DagBag()

def test_dag_loaded(dag_bag):
    """
    Basic test to ensure the DAG file can be successfully imported.
    This helps catch syntax errors and other issues in the DAG definition.
    """
    dag_id = 'FlightsETL'  # Replace with your actual DAG ID
    dag = dag_bag.get_dag(dag_id)
    assert dag_bag.import_errors == {}, "Errors were found loading DAGs!"
    assert dag is not None, f"DAG with id {dag_id} not found"
    assert len(dag.tasks) > 0, "DAG has no tasks"

def test_task_execution():
    """Task Execution Test focuses on executing a specific task within a DAG to ensure it can run to completion successfully. This test can be extended or modified to check for specific output values or states depending on what your task does."""
    # Import the DAG from your project
    from flights_etl import dag as my_dag
    
    # Retrieve the specific task
    task = my_dag.get_task('BIGQUERY_raw_to_datamart')
    
    # Simulate execution of the task
    ti = TaskInstance(task=task, execution_date=datetime.now())
    context = ti.get_template_context()
    ti.run(ignore_ti_state=True)  # Set ignore_ti_state to True to not check the previous state of the TaskInstance
    
    # Verify the task succeeded
    assert ti.state == 'success', "Task execution failed"