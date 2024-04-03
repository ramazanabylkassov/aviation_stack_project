import pytest
from unittest.mock import patch
from airflow.models import DagBag, TaskInstance, DagRun
from airflow.models.dagrun import DagRunType
from datetime import datetime, timedelta
from airflow.utils.session import create_session
from airflow.utils.state import State
import pytz
from flights_etl import dag as my_dag  

def test_dag_import():
    """DAG Import Test checks if all DAGs in the specified folder can be imported without any errors. This helps catch syntax errors, import failures, or other initialisation issues."""

    dag_bag = DagBag(dag_folder='dags/', include_examples=False)
    assert dag_bag.import_errors == {}, "DAG import failed"

@pytest.fixture
def mock_dependencies():
    """Mock the dependencies of the task."""
    with patch('airflow.models.TaskInstance.are_dependencies_met') as mock_dependencies_met:
        mock_dependencies_met.return_value = True
        yield

def test_task_execution(mock_dependencies):
    """Task Execution Test focuses on executing a specific task within a DAG to ensure it can run to completion successfully."""
    # Import the DAG from your project
    execution_date = datetime.now(pytz.utc)

    with create_session() as session:
        dag_run = DagRun(
            dag_id='FlightsETL',
            run_id='test_run',
            execution_date=execution_date,
            start_date=execution_date,
            state=State.RUNNING,
            run_type=DagRunType.MANUAL
        )
        session.add(dag_run)
        session.commit()

    # Retrieve the specific task
    task = my_dag.get_task('BIGQUERY_raw_to_datamart')
    
    # Simulate execution of the task
    ti = TaskInstance(task=task, run_id='test_run')
    context = ti.get_template_context()
    ti.run(ignore_ti_state=True)  # Set ignore_ti_state to True to not check the previous state of the TaskInstance
    
    # Verify the task succeeded
    assert ti.state == 'success', "Task execution failed"

# update