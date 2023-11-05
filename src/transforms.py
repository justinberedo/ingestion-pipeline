import pandas as pd
import logging


logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)


def lowercase_emails(dataframe: pd.DataFrame) -> pd.DataFrame:
    dataframe['email'] = dataframe['email'].str.lower()
    return dataframe


def uppercase_product_names(dataframe: pd.DataFrame) -> pd.DataFrame:
    dataframe['product'] = dataframe['product'].str.upper()
    return dataframe


def get_new_records(
    dataframe: pd.DataFrame,
    latest_id: int,
    id_column: str,
) -> pd.DataFrame:
    return dataframe[dataframe[id_column] > latest_id]


def sum_amount_per_user(df_transactions: pd.DataFrame):
    return df_transactions.groupby('user_id')['amount'].sum().reset_index()


def get_new_total_spent_per_user(
    df_sum_per_user: pd.DataFrame,
    df_current_user_amount: pd.DataFrame
):
    df_new_total_spent = pd.merge(
        df_current_user_amount,
        df_sum_per_user,
        on='user_id',
        how='left'
    )
    df_new_total_spent['amount'].fillna(0, inplace=True)
    df_new_total_spent['total_spent'] += df_new_total_spent['amount']
    df_new_total_spent = df_new_total_spent.drop(columns='amount')
    df_new_total_spent['date_joined'] = (
        df_new_total_spent['date_joined'].astype(str)
    )
    return df_new_total_spent
