import logging
import os
from os.path import exists, join

import pandas as pd

from src.util.config import (
    AVG_BITCOIN_DIFF_CSV_FORMAT,
    AVG_BITCOIN_DIFF_DATA_LOCATION,
    BITCOIN_COMPARISON_DATA_LOCATION,
)
from src.util.dataframe_ops import read_csv, write_csv

logger = logging.getLogger(__name__)


class AverageDifferenceStep:

    def __init__(self, timestamp: str):
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

    def generate_average_difference(self) -> pd.DataFrame:
        if exists(self.avg_bitcoin_diff_csv):
            logger.info(
                f"Dataset already exists at '{self.avg_bitcoin_diff_csv}'. Using pre-existing dataset instead of generating new dataset. \nIf you desire to generate a new dataset re-run without providing a timestamp."
            )
        else:
            comparisons_df = self.read_all_bitcoin_comparisons()

            # Group by symbol across all datasets and get average difference
            # in 24h percent change against bitcoin
            avg_comparisons_df = (
                comparisons_df.groupby("symbol")["24h_against_bitcoin"]
                .mean()
                .reset_index()
            )

            bitcoin_diff_df = avg_comparisons_df.rename(
                columns={"24h_against_bitcoin": "Avg_24h_Bitcoin_Diff"}
            )

            write_csv(self.avg_bitcoin_diff_csv, bitcoin_diff_df)
            return bitcoin_diff_df

    def read_all_bitcoin_comparisons(self) -> pd.DataFrame:
        comparison_dfs = []

        for file_name in os.listdir(self.bitcoin_comparison_directory):
            if file_name.endswith(".csv"):
                df = read_csv(join(self.bitcoin_comparison_directory, file_name))
                comparison_dfs.append(df)

        all_dfs = pd.concat(comparison_dfs, ignore_index=True)
        return all_dfs
