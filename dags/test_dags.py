# test_dag_integrity.py

import pytest
from airflow.models import DagBag

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

# --Update 1