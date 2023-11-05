import logging
import pandas as pd

logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)


def check_for_missing_values(dataframe: pd.DataFrame):
    if dataframe.isnull().values.any():
        logging.error("DataFrame contains missing values.")
        raise ValueError("DataFrame contains missing values.")
