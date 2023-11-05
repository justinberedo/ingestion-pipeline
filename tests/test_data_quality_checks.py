from src.data_quality_checks import check_for_missing_values
import pandas as pd
import pytest
import os
import sys
sys.path.insert(0, os.path.abspath('src'))


@pytest.fixture()
def example_data():
    return {
        'user_id': [1, 2, 3, 4, 5],
        'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
        'email': [
            'alice@example.com',
            'bob@example.com',
            'charlie@example.com',
            'david@example.com',
            'eve@example.com'
        ],
        'date_joined': [
            '2022-01-01',
            '2022-02-02',
            '2022-03-03',
            '2022-04-04',
            '2022-05-05'
        ]
    }


def test_check_for_missing_values_success(example_data):
    df = pd.DataFrame(example_data)
    result = check_for_missing_values(dataframe=df)
    assert result is True


def test_check_for_missing_values_failure(example_data):
    example_data['user_id'][2] = None
    df = pd.DataFrame(example_data)
    with pytest.raises(ValueError):
        check_for_missing_values(df)
