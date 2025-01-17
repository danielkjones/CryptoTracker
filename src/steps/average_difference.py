import os
from os.path import dirname, join

import pandas as pd


class AverageDifferenceStep:

    def __init__(self, timestamp: str):
        self.timestamp = timestamp
        self.bitcoin_comparison_directory = join(
            dirname(dirname(dirname(__file__))), "data_lake/bitcoin_comparison"
        )

        self.avg_bitcoin_diff_directory = join(
            dirname(dirname(dirname(__file__))), "data_lake/avg_bitcoin_diff"
        )
        self.avg_bitcoin_diff_file_format = "avg_bitcoin_diff_{}.csv"

    def generate_average_difference(self) -> pd.DataFrame:
        comparisons_df = self.read_all_bitcoin_comparisons()

        avg_comparisons_df = (
            comparisons_df.groupby("symbol")["24h_against_bitcoin"].mean().reset_index()
        )

        bitcoin_diff_df = avg_comparisons_df.rename(
            columns={"24h_against_bitcoin": "Avg_24h_Bitcoin_Diff"}
        )

        self.write_dataframe(bitcoin_diff_df)

        return bitcoin_diff_df

    def read_all_bitcoin_comparisons(self) -> pd.DataFrame:
        comparison_dfs = []

        for file_name in os.listdir(self.bitcoin_comparison_directory):
            if file_name.endswith(".csv"):
                df = pd.read_csv(join(self.bitcoin_comparison_directory, file_name))
                comparison_dfs.append(df)

        all_dfs = pd.concat(comparison_dfs, ignore_index=True)
        return all_dfs

    def write_dataframe(self, df: pd.DataFrame) -> None:
        dataset_path = join(
            self.avg_bitcoin_diff_directory,
            self.avg_bitcoin_diff_file_format.format(self.timestamp),
        )
        df.to_csv(dataset_path)
