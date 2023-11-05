import pandas as pd
from src.postgresql_db import PostgreSQLDatabase
import logging


logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)


def read_csv(file_path: str) -> pd.DataFrame:
    try:
        df = pd.read_csv(file_path)
        logging.info("Number of records from %s: %s", file_path, df.shape[0])
        return df
    except Exception as e:
        logging.error(e)
        raise Exception(e)


def get_latest_id(
    db_instance: PostgreSQLDatabase,
    table: str,
    id_column: str,
):
    try:
        query = f"""
            SELECT MAX({id_column})
            FROM {table};
        """
        latest_id = db_instance.query(query=query)[0]['max']
        logging.info(f"Latest user_id in {table} table: {latest_id}")
        return latest_id
    except Exception as e:
        logging.error(e)
        raise Exception(e)


def get_current_user_amount(
    db_instance: PostgreSQLDatabase,
    user_ids: list
):
    try:
        user_ids_str = ", ".join([str(user_id) for user_id in user_ids])
        query = f"""
            SELECT *
            FROM users
            WHERE user_id IN ({user_ids_str});
        """
        results = db_instance.query(query=query)
        return pd.DataFrame(results)
    except Exception as e:
        logging.error(e)
        raise Exception(e)
