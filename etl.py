from src.extracts import (
    read_csv,
    get_latest_id,
    get_current_user_amount,
)
from src.data_quality_checks import check_for_missing_values
from src.transforms import (
    lowercase_emails,
    uppercase_product_names,
    get_new_records,
    sum_amount_per_user,
    get_new_total_spent_per_user,
)
from src.load import insert_to_table, update_total_spent
from src.postgresql_db import PostgreSQLDatabase
import logging


logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logging.getLogger().setLevel(logging.INFO)

USERS_FILE_PATH = "data_sources/users.csv"
TRANSACTIONS_FILE_PATH = "data_sources/transactions.csv"


def etl(
    users_file_path: str,
    transactions_file_path: str,
    db_instance: PostgreSQLDatabase,
) -> None:
    users_table_name = 'users'
    users_columns = [
        'user_id',
        'name',
        'email',
        'date_joined',
        'total_spent',
    ]
    transactions_table_name = 'transactions'
    transactions_columns = [
        'trans_id',
        'user_id',
        'product',
        'amount',
        'trans_date',
    ]

    df_users = read_csv(users_file_path)
    check_for_missing_values(df_users)
    df_transactions = read_csv(transactions_file_path)
    check_for_missing_values(df_transactions)

    latest_user_id = get_latest_id(
        db_instance=db_instance,
        table=users_table_name,
        id_column=users_columns[0],  # user_id
    )

    latest_transaction_id = get_latest_id(
        db_instance=db_instance,
        table=transactions_table_name,
        id_column=transactions_columns[0],  # trans_id
    )

    if latest_user_id:
        df_users = get_new_records(
            dataframe=df_users,
            latest_id=latest_user_id,
            id_column=users_columns[0],
        )

    if latest_transaction_id:
        df_transactions = get_new_records(
            dataframe=df_transactions,
            latest_id=latest_transaction_id,
            id_column=transactions_columns[0],
        )

    # Transformations and load new data to table
    if len(df_users) == 0:
        logging.info("No new users.")
    else:
        lowercase_emails(dataframe=df_users)
        df_users['total_spent'] = 0
        insert_to_table(
            dataframe=df_users,
            db_instance=db_instance,
            table_name='users',
            columns=users_columns,
        )

    if len(df_transactions) == 0:
        logging.info("No new transactions.")
    else:
        uppercase_product_names(dataframe=df_transactions)
        insert_to_table(
            dataframe=df_transactions,
            db_instance=db_instance,
            table_name='transactions',
            columns=transactions_columns,
        )

        # Update total_spent
        df_sum_per_user = sum_amount_per_user(df_transactions=df_transactions)
        print(df_sum_per_user)
        df_current_user_amount = get_current_user_amount(
            db_instance=db_instance,
            user_ids=df_sum_per_user['user_id'].tolist()
        )
        print(df_current_user_amount)
        df_new_total_spent = get_new_total_spent_per_user(
            df_sum_per_user=df_sum_per_user,
            df_current_user_amount=df_current_user_amount
        )
        print(df_new_total_spent)
        update_total_spent(
            dataframe=df_new_total_spent,
            db_instance=db_instance
        )


if __name__ == "__main__":
    db_instance = PostgreSQLDatabase(
        dbname='first_circle_db',
        host='localhost',
        port=5432,
        user='db_user',
        password='db_password',
    )
    # In real world systems, passwords should be
    # retrieved from secrets managers
    # and/or read as environment variables.

    etl(
        users_file_path=USERS_FILE_PATH,
        transactions_file_path=TRANSACTIONS_FILE_PATH,
        db_instance=db_instance,
    )
