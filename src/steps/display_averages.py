from os.path import join

from tabulate import tabulate

from src.util.config import AVG_BITCOIN_DIFF_CSV_FORMAT, AVG_BITCOIN_DIFF_DATA_LOCATION
from src.util.dataframe_ops import read_csv


class DisplayAveragesStep:

    def __init__(self, timestamp: str):
        self.timestamp = timestamp
        # input dataset details
        self.avg_bitcoin_diff_directory = AVG_BITCOIN_DIFF_DATA_LOCATION
        self.avg_bitcoin_diff_file_format = AVG_BITCOIN_DIFF_CSV_FORMAT

    @property
    def avg_bitcoin_diff_csv(self) -> str:
        return join(
            self.avg_bitcoin_diff_directory,
            self.avg_bitcoin_diff_file_format.format(self.timestamp),
        )

    def display_averages(self) -> None:
        """Simple Function to read in the workflow output and display to the end user"""
        avg_diff_df = read_csv(self.avg_bitcoin_diff_csv)
        # pretty printing the data frame using tabulate
        print(
            "\nDisplaying average difference between the 24 hour percent change of each "
            "coin \nand the 24 hour percent change of Bitcoin, across all executions. \nA "
            "POSITIVE value means the coin has had a higher percentage change than Bitcoin."
            "\nA NEGATIVE value means the coin has had a lower percentage change than Bitcoin."
        )
        print(
            "\n\n"
            + tabulate(
                avg_diff_df, headers="keys", tablefmt="rounded_grid", showindex=False
            )
        )
