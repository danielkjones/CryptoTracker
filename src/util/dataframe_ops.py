import logging

import pandas as pd

logger = logging.getLogger(__name__)


def read_csv(file_path: str) -> pd.DataFrame:
    """Helper function to read a .csv dataset, or log out the error to
    end user in the case of an error.

    Args:
        file_path (str): File path of a .csv file to read

    Raises:
        e: Exception from an attempt to read the dataset

    Returns:
        pd.DataFrame: DataFrame representation of the dataset
    """
    try:
        return pd.read_csv(file_path)
    except Exception as e:
        logger.error(
            f"ERROR reading dataset at '{file_path}'. Fix input location and re-run process."
        )
        raise e


def write_csv(file_path: str, dataframe: pd.DataFrame) -> None:
    """Helper function to write a dataframe to .csv, or log out the error
    to the end user in the case of an error.

    Args:
        file_path (str): File to write the .csv to
        dataframe (pd.DataFrame): DataFrame to write out

    Raises:
        e: Exception from an attempt to write the dataset
    """
    try:
        dataframe.to_csv(file_path, index=False)
    except Exception as e:
        logger.error(
            f"ERROR writing dataset to file path '{file_path}'. Fix output location and re-run process."
        )
        raise e
