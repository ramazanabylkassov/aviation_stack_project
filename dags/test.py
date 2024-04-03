from airflow.models import DagBag, TaskInstance
from datetime import datetime

def test_dag_import():
    """DAG Import Test checks if all DAGs in the specified folder can be imported without any errors. This helps catch syntax errors, import failures, or other initialisation issues."""

    dag_bag = DagBag(dag_folder='path/to/your/dag/directory', include_examples=False)
    assert dag_bag.import_errors == {}, "DAG import failed"

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

# update