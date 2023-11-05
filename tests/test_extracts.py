from src.extracts import (
    read_csv,
    get_latest_id,
    get_current_user_amount,
)
import pandas as pd
import pytest
import os
import sys
from unittest.mock import Mock
sys.path.insert(0, os.path.abspath('src'))


def test_read_csv_valid_file():
    file_path = 'tests/test_users.csv'
    result = read_csv(file_path)
    assert isinstance(result, pd.DataFrame)


def test_read_csv_invalid_file():
    file_path = 'data_sources/non_existent_file.csv'
    with pytest.raises(Exception):
        read_csv(file_path)


def test_get_latest_id_success():
    mock_query = Mock()
    mock_query.return_value = [{'max': 42}]

    mock_db_instance = Mock()
    mock_db_instance.query = mock_query

    latest_id = get_latest_id(mock_db_instance, 'your_table', 'user_id')

    assert latest_id == 42


def test_get_latest_id_failure():
    mock_query = Mock(side_effect=Exception("Database error"))

    mock_db_instance = Mock()
    mock_db_instance.query = mock_query

    with pytest.raises(Exception):
        get_latest_id(mock_db_instance, 'your_table', 'user_id')


def test_get_current_user_amount_success():
    mock_query = Mock()
    mock_query.return_value = [
        {'user_id': 1, 'name': 'Alice'},
        {'user_id': 2, 'name': 'Bob'},
    ]

    mock_db_instance = Mock()
    mock_db_instance.query = mock_query

    user_ids = [1, 2]
    result_df = get_current_user_amount(mock_db_instance, user_ids)

    expected_data = pd.DataFrame([
        {'user_id': 1, 'name': 'Alice'},
        {'user_id': 2, 'name': 'Bob'},
    ])
    pd.testing.assert_frame_equal(result_df, expected_data)


def test_get_current_user_amount_failure():
    mock_query = Mock(side_effect=Exception("Database error"))

    mock_db_instance = Mock()
    mock_db_instance.query = mock_query

    with pytest.raises(Exception):
        get_current_user_amount(mock_db_instance, [1, 2])
