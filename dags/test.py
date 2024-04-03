import pytest
from unittest.mock import patch, Mock
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

# update