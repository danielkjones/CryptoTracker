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
    PRICING_CSV_FORMAT,
    PRICING_DATA_LOCATION,
)
from src.util.dataframe_ops import read_csv, write_csv

logger = logging.getLogger(__name__)


class BitcoinComparisonStep:

    def __init__(self, timestamp: str):
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

        if exists(self.bitcoin_comparison_csv):
            logger.info(
                f"Dataset already exists at '{self.bitcoin_comparison_csv}'. Using pre-existing dataset instead of generating new dataset. \nIf you desire to generate a new dataset re-run without providing a timestamp."
            )
        else:
            pricing_df = read_csv(self.pricing_csv)
            pricing_df["bitcoin_percent_change_24h"] = self.bitcoin_percent_change_24h()

            # Finding difference. NEGATIVE value means that the coin changed LESS than bitcoin. POSITIVE value means
            # the coin changed MORE than bitcoin.
            pricing_df["24h_against_bitcoin"] = (
                pricing_df["quote.USD.percent_change_24h"]
                - pricing_df["bitcoin_percent_change_24h"]
            )

            comparison_df = self.trim_df_values(pricing_df)
            sorted_df = self.sort_df(comparison_df)

            write_csv(self.bitcoin_comparison_csv, sorted_df)
            return sorted_df

    def trim_df_values(self, comparison_df: pd.DataFrame) -> pd.DataFrame:
        """There are only a few values that we want to have in this dataset
        to avoid cluttering for the end user

        Returns:
            pd.DataFrame: DataFrame with extraneous information removed
        """
        return comparison_df[
            [
                "symbol",
                "name",
                "24h_against_bitcoin",
                "quote.USD.percent_change_24h",
                "bitcoin_percent_change_24h",
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
            by="24h_against_bitcoin", ascending=True
        ).reset_index(drop=True)

    def bitcoin_percent_change_24h(self) -> float:
        # Need to get the value of bitcoin from one of the other layers
        # It may be worthwhile modifying in the near future to have a
        # clean set of listings... TBD
        listing_df = read_csv(self.listings_csv)
        # get the bitcoin quote from the listing df
        bitcoin_change = listing_df.loc[listing_df["name"] == "Bitcoin"][
            "quote.USD.percent_change_24h"
        ]
        return bitcoin_change
