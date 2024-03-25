# Import the functions from your DAG file
from dag_demo import print_hello, print_world

def test_print_hello():
    """Test that the print_hello function returns 'Hello'."""
    assert print_hello() == 'Hello', "print_hello should return 'Hello'"

def test_print_world():
    """Test that the print_world function returns 'World'."""
    assert print_world() == 'World', "print_world should return 'World'"
