import logging
import os
from os.path import exists, join
from typing import Optional

import pandas as pd

from src.util.config import (
    AVG_BITCOIN_DIFF_CSV_FORMAT,
    AVG_BITCOIN_DIFF_DATA_LOCATION,
    BITCOIN_COMPARISON_DATA_LOCATION,
    LOGGER_NAME,
)
from src.util.dataframe_ops import read_csv, write_csv

logger = logging.getLogger(LOGGER_NAME)


class AverageDifferenceStep:

    def __init__(self, timestamp: str):
        """Step object that calculates the average difference of 24 hour percent change of the
        a currency vs. Bitcoin. Writes output to data lake as .csv.

        Args:
            timestamp (str): UTC Timestamp of execution in YYYYMMDDHHMMSS
        """
        self.timestamp = timestamp
        # input dataset details
        self.bitcoin_comparison_directory = BITCOIN_COMPARISON_DATA_LOCATION
        # output dataset details
        self.avg_bitcoin_diff_directory = AVG_BITCOIN_DIFF_DATA_LOCATION
        self.avg_bitcoin_diff_file_format = AVG_BITCOIN_DIFF_CSV_FORMAT

    @property
    def avg_bitcoin_diff_csv(self) -> str:
        return join(
            self.avg_bitcoin_diff_directory,
            self.avg_bitcoin_diff_file_format.format(self.timestamp),
        )

    def generate_average_difference(self) -> Optional[pd.DataFrame]:
        """Reads all Bitcoin comparison files that pre-exist and calculates the average
        difference for each symbol for the collected data. Sorts in ascending order based
        on average. Writes output to data lake.

        If there already exists a file for the given execution timestamp, skips step to
        avoid repeat work.

        Returns:
            Optional[pd.DataFrame]: DataFrame with averages per symbol if calculated. None if
                file already existed.
        """
        if exists(self.avg_bitcoin_diff_csv):
            logger.info(
                f"Dataset already exists at '{self.avg_bitcoin_diff_csv}'. Using pre-existing"
                " dataset instead of generating new dataset. \nIf you desire to generate a new dataset re-run without providing a timestamp."
            )
        else:
            comparisons_df = self.read_all_bitcoin_comparisons()

            # Group by symbol across all datasets and get average difference
            # in 24h percent change against bitcoin
            avg_comparisons_df = (
                comparisons_df.groupby("Symbol")[
                    "BitcoinVsCurrency24hPercentChangeDiff"
                ]
                .mean()
                .reset_index()
            )

            bitcoin_diff_df = avg_comparisons_df.rename(
                columns={
                    "BitcoinVsCurrency24hPercentChangeDiff": "AvgBitcoinVsCurrency24hPercentChangeDiff"
                }
            ).sort_values("AvgBitcoinVsCurrency24hPercentChangeDiff", ascending=True)

            write_csv(self.avg_bitcoin_diff_csv, bitcoin_diff_df)
            return bitcoin_diff_df

    def read_all_bitcoin_comparisons(self) -> pd.DataFrame:
        """Read the individual datasets from the Bitcoin comparison dataset
        directory and return all values as a singular dataframe.

        Returns:
            pd.DataFrame: dataframe with all collected comparison data
        """
        comparison_dfs = []

        for file_name in os.listdir(self.bitcoin_comparison_directory):
            if file_name.endswith(".csv"):
                df = read_csv(join(self.bitcoin_comparison_directory, file_name))
                comparison_dfs.append(df)

        all_dfs = pd.concat(comparison_dfs, ignore_index=True)
        return all_dfs
