import ast
import logging
from os.path import exists, join
from typing import Optional

import pandas as pd

from src.util.config import (
    BITCOIN_COMPARISON_CSV_FORMAT,
    BITCOIN_COMPARISON_DATA_LOCATION,
    LISTINGS_CSV_FORMAT,
    LISTINGS_DATA_LOCATION,
    LOGGER_NAME,
    PRICING_CSV_FORMAT,
    PRICING_DATA_LOCATION,
)
from src.util.dataframe_ops import read_csv, write_csv

logger = logging.getLogger(LOGGER_NAME)


class BitcoinComparisonStep:

    def __init__(self, timestamp: str):
        """Step object that takes in pricing data and calculates the difference of
        24 hour percent change of a currency vs. Bitcoin. Writes output to data lake
        as .csv.

        Args:
            timestamp (str): UTC Timestamp of execution in YYYYMMDDHHMMSS
        """
        self.timestamp = timestamp
        # input dataset details
        self.pricing_file_directory = PRICING_DATA_LOCATION
        self.pricing_file_format = PRICING_CSV_FORMAT

        self.listings_file_directory = LISTINGS_DATA_LOCATION
        self.listings_file_format = LISTINGS_CSV_FORMAT
        # output dataset details
        self.bitcoin_comparison_directory = BITCOIN_COMPARISON_DATA_LOCATION
        self.bitcoin_comparison_file_format = BITCOIN_COMPARISON_CSV_FORMAT

    @property
    def pricing_csv(self) -> str:
        return join(
            self.pricing_file_directory,
            self.pricing_file_format.format(self.timestamp),
        )

    @property
    def listings_csv(self) -> str:
        return join(
            self.listings_file_directory,
            self.listings_file_format.format(self.timestamp),
        )

    @property
    def bitcoin_comparison_csv(self) -> str:
        return join(
            self.bitcoin_comparison_directory,
            self.bitcoin_comparison_file_format.format(self.timestamp),
        )

    def generate_bitcoin_comparison(self) -> Optional[pd.DataFrame]:
        """Generates Bitcoin comparison file from pricing file input.
        Adds in Bitcoin percent change data for reference. Removes unneeded pricing
        data. Writes output to data lake.

        If there already exists a file for the given execution timestamp, skips step to
        avoid repeat work.

        Returns:
            Optional[pd.DataFrame]: Dataframe with Bitcoin comparison data for the given
                symbols. None if the file already exists.
        """

        if exists(self.bitcoin_comparison_csv):
            logger.info(
                f"Dataset already exists at '{self.bitcoin_comparison_csv}'. Using pre-existing dataset instead of generating new dataset. \nIf you desire to generate a new dataset re-run without providing a timestamp."
            )
        else:
            pricing_df = read_csv(self.pricing_csv)
            pricing_df["BitcoinPercentChange24h"] = self.bitcoin_percent_change_24h()

            # Finding difference. NEGATIVE value means that the coin changed LESS than bitcoin. POSITIVE value means
            # the coin changed MORE than bitcoin.
            pricing_df["BitcoinVsCurrency24hPercentChangeDiff"] = (
                pricing_df["PercentChange24h"] - pricing_df["BitcoinPercentChange24h"]
            )

            comparison_df = self.trim_df_values(pricing_df)
            sorted_df = self.sort_df(comparison_df)

            write_csv(self.bitcoin_comparison_csv, sorted_df)
            return sorted_df

    def trim_df_values(self, comparison_df: pd.DataFrame) -> pd.DataFrame:
        """There are only a few values that we want to have in this dataset
        to avoid cluttering for the end user.

        Saving information to track the identity of the coin, time of the
        data load, and numbers related to percent change over the last
        24 hours.

        Returns:
            pd.DataFrame: DataFrame with extraneous information removed
        """
        return comparison_df[
            [
                "ID",
                "Symbol",
                "Name",
                "BitcoinVsCurrency24hPercentChangeDiff",
                "PercentChange24h",
                "BitcoinPercentChange24h",
                "LoadedWhen",
            ]
        ]

    def sort_df(self, comparison_df: pd.DataFrame) -> pd.DataFrame:
        """Need to sort the dataframe from smallest difference to largest
        difference

        Args:
            comparison_df (pd.DataFrame): Unsorted Comparison DataFrame

        Returns:
            pd.DataFrame: DF Sorted by the difference from smallest to largest
        """
        return comparison_df.sort_values(
            by="BitcoinVsCurrency24hPercentChangeDiff", ascending=True
        ).reset_index(drop=True)

    def bitcoin_percent_change_24h(self) -> float:
        """Pull the bitcoin percent change from the raw listings data in the
        data lake.

        Returns:
            float: Bitcoin percent change in last 24 hours
        """
        # Get the bitcoin percent change from the listings dataset
        listing_df = read_csv(self.listings_csv)
        # get the bitcoin quote from the listing df
        bitcoin_change: pd.Series = listing_df.loc[listing_df["name"] == "Bitcoin"][
            "quote.USD.percent_change_24h"
        ]
        return float(bitcoin_change.values[0])
