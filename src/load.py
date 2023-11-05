import logging
import pandas as pd
from postgresql_db import PostgreSQLDatabase
from io import StringIO


logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)


def insert_to_table(
    dataframe: pd.DataFrame,
    db_instance: PostgreSQLDatabase,
    table_name: str,
    columns: list,
):
    try:
        sio = StringIO()
        sio.write(dataframe.to_csv(index=None, header=None))
        sio.seek(0)
        db_instance.cur.copy_from(
            file=sio,
            table=table_name,
            columns=columns,
            sep=","
        )
        db_instance.conn.commit()
        logging.info(
            "Number of records loaded to %s table: %s",
            table_name,
            dataframe.shape[0],
        )
    except Exception as e:
        logging.error(e)


def update_total_spent(
    dataframe: pd.DataFrame,
    db_instance: PostgreSQLDatabase,
):
    try:
        query = f"""
            INSERT INTO users (user_id, name, email, date_joined, total_spent)
            VALUES {','.join(
                [str(i) for i in list(dataframe.to_records(index=False))]
                )}
            ON CONFLICT (user_id)
            DO UPDATE SET total_spent = excluded.total_spent
        """
        db_instance.query(
            query=query,
            returning=False,
        )
        logging.info(
            "Number of records updated in users table: %s",
            dataframe.shape[0],
        )
    except Exception as e:
        logging.error(e)
