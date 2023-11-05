from src.transforms import (
    lowercase_emails,
    uppercase_product_names,
    get_new_records,
    sum_amount_per_user,
    get_new_total_spent_per_user,
)
import pandas as pd
import os
import sys
sys.path.insert(0, os.path.abspath('src'))


def test_lowercase_emails_success():
    data = {
        'user_id': [1, 2, 3],
        'name': ['Alice', 'Bob', 'Charlie'],
        'email': [
            'alice@example.com',
            'BOB@example.com',
            'CharLiE@example.com'
        ]
    }
    df = pd.DataFrame(data)

    result_df = lowercase_emails(df)

    assert all(result_df['email'] == [
        'alice@example.com',
        'bob@example.com',
        'charlie@example.com'
    ])


def test_uppercase_product_names_success():
    data = {
        'trans_id': [1, 2, 3],
        'product': ['iPhone', 'Macbook', 'Kindle']
    }
    df = pd.DataFrame(data)

    result_df = uppercase_product_names(df)

    assert all(result_df['product'] == ['IPHONE', 'MACBOOK', 'KINDLE'])


def test_get_new_records_success():
    data = {
        'user_id': [1, 2, 3, 4, 5],
        'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve']
    }
    df = pd.DataFrame(data)
    latest_id = 3

    result_df = get_new_records(df, latest_id, 'user_id')

    assert all(result_df['user_id'] == [4, 5])


def test_sum_amount_per_user_success():
    data = {
        'user_id': [1, 2, 1, 3, 2, 3],
        'amount': [100, 200, 150, 50, 300, 75]
    }
    df_transactions = pd.DataFrame(data)

    result_df = sum_amount_per_user(df_transactions)

    expected_data = {
        'user_id': [1, 2, 3],
        'amount': [250, 500, 125]
    }
    expected_df = pd.DataFrame(expected_data)

    pd.testing.assert_frame_equal(result_df, expected_df)


def test_get_new_total_spent_per_user_success():
    data_sum_per_user = {
        'user_id': [1, 2, 3],
        'amount': [100, 200, 300],
    }
    df_sum_per_user = pd.DataFrame(data_sum_per_user)

    data_current_user_amount = {
        'user_id': [1, 2, 3],
        'total_spent': [50, 150, 75],
        'date_joined': ['2023-11-02', '2023-11-03', '2023-11-04'],
    }
    df_current_user_amount = pd.DataFrame(data_current_user_amount)

    result_df = get_new_total_spent_per_user(
        df_sum_per_user,
        df_current_user_amount
    )

    expected_data = {
        'user_id': [1, 2, 3],
        'total_spent': [150, 350, 375],
        'date_joined': ['2023-11-02', '2023-11-03', '2023-11-04']
    }
    expected_df = pd.DataFrame(expected_data)

    pd.testing.assert_frame_equal(result_df, expected_df)
