from src.load import (
    insert_to_table,
    update_total_spent,
)
import pandas as pd
import pytest
import os
import sys
from unittest.mock import Mock
sys.path.insert(0, os.path.abspath('src'))


@pytest.fixture()
def mock_database():
    class MockDatabase:
        def __init__(self):
            self.cur = Mock()
            self.conn = Mock()
            self.query = Mock()
    return MockDatabase()


def test_insert_to_table_success(mock_database):
    mock_db_instance = mock_database

    data = {
        'Column1': [1, 2, 3, 4, 5],
        'Column2': ['A', 'B', 'C', 'D', 'E']
    }
    df = pd.DataFrame(data)

    table_name = 'your_table'
    columns = ['Column1', 'Column2']
    insert_to_table(df, mock_db_instance, table_name, columns)

    expected_data = '1,A\n2,B\n3,C\n4,D\n5,E\n'

    file_object = mock_db_instance.cur.copy_from.call_args[1]['file']
    actual_data = file_object.getvalue()

    assert actual_data == expected_data


def test_insert_to_table_failure(mock_database):
    mock_db_instance = mock_database
    mock_db_instance.cur.copy_from.side_effect = Exception("Database error")

    data = {
        'Column1': [1, 2, 3, 4, 5],
        'Column2': ['A', 'B', 'C', 'D', 'E']
    }
    df = pd.DataFrame(data)

    table_name = 'your_table'
    columns = ['Column1', 'Column2']
    with pytest.raises(Exception):
        insert_to_table(df, mock_db_instance, table_name, columns)


@pytest.fixture()
def example_data():
    return {
        'user_id': [1, 2, 3],
        'name': ['Alice', 'Bob', 'Charlie'],
        'email': [
            'alice@example.com',
            'bob@example.com',
            'charlie@example.com'
        ],
        'date_joined': ['2022-01-01', '2022-02-02', '2022-03-03'],
        'total_spent': [100, 200, 300]
    }


def test_update_total_spent_success(mock_database, example_data):
    mock_db_instance = mock_database

    df = pd.DataFrame(example_data)

    update_total_spent(df, mock_db_instance)

    expected_query = """
            INSERT INTO users (user_id, name, email, date_joined, total_spent)
            VALUES (1, 'Alice', 'alice@example.com', '2022-01-01', 100),(2, 'Bob', 'bob@example.com', '2022-02-02', 200),(3, 'Charlie', 'charlie@example.com', '2022-03-03', 300)
            ON CONFLICT (user_id)
            DO UPDATE SET total_spent = excluded.total_spent
        """
    mock_db_instance.query.assert_called_once_with(
        query=expected_query,
        returning=False,
    )


def test_update_total_spent_failure(mock_database, example_data):
    mock_db_instance = mock_database
    mock_db_instance.query.side_effect = Exception("Database error")

    df = pd.DataFrame(example_data)

    with pytest.raises(Exception):
        update_total_spent(df, mock_db_instance)
