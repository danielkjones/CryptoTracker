import ast
import json
from os.path import dirname, join

import pandas as pd


class BitcoinComparisonStep:

    def __init__(self, timestamp: str):
        self.timestamp = timestamp

        self.pricing_file_directory = join(
            dirname(dirname(dirname(__file__))), "data_lake/pricing"
        )
        self.pricing_file_format = "coins_pricing_{}.csv"
        self.listings_file_directory = join(
            dirname(dirname(dirname(__file__))), "data_lake/listings"
        )

        self.listings_file_format = "crypto_listings_{}.csv"
        self.bitcoin_comparison_directory = join(
            dirname(dirname(dirname(__file__))), "data_lake/bitcoin_comparison"
        )
        self.bitcoin_comparison_file_format = "bitcoin_comparison_{}.csv"

    def generate_bitcoin_comparison(self) -> pd.DataFrame:
        pricing_df = self.read_pricing()
        pricing_df["percent_change_24h"] = pricing_df["quote"].apply(
            self.unpack_percent_change_24h
        )

        bitcoin_percentage_change = self.bitcoin_percent_change_24h()
        pricing_df["bitcoin_percent_change_24h"] = bitcoin_percentage_change

        # Finding difference. NEGATIVE value means that the coin changed LESS than bitcoin. POSITIVE value means
        # the coin changed MORE than bitcoin.
        pricing_df["24h_against_bitcoin"] = (
            pricing_df["percent_change_24h"] - pricing_df["bitcoin_percent_change_24h"]
        )

        comparison_df = self.trim_df_values(pricing_df)
        sorted_df = self.sort_df(comparison_df)

        self.write_df(sorted_df)
        return sorted_df

    def read_pricing(self) -> pd.DataFrame:
        df = pd.read_csv(
            join(
                self.pricing_file_directory,
                self.pricing_file_format.format(self.timestamp),
            )
        )
        return df

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
                "percent_change_24h",
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

    def write_df(self, comparison_df: pd.DataFrame) -> None:
        dataset_path = join(
            self.bitcoin_comparison_directory,
            self.bitcoin_comparison_file_format.format(self.timestamp),
        )
        comparison_df.to_csv(dataset_path)

    def bitcoin_percent_change_24h(self) -> float:
        # Need to get the value of bitcoin from one of the other layers
        # It may be worthwhile modifying in the near future to have a
        # clean set of listings... TBD
        listing_df = pd.read_csv(
            join(
                self.listings_file_directory,
                self.listings_file_format.format(self.timestamp),
            )
        )
        # get the bitcoin quote from the listing df
        bitcoin_quote = listing_df.loc[listing_df["name"] == "Bitcoin"]["quote"][0]
        bitcoin_percent_change_24h = self.unpack_percent_change_24h(bitcoin_quote)
        return bitcoin_percent_change_24h

    def unpack_percent_change_24h(self, quote: str):
        # Unpack price information
        # Need to use ast due to the formatting constraints of pandas .to_csv.
        # May be worth investigating a better way in the future
        jsn = ast.literal_eval(quote)
        return jsn.get("USD").get("percent_change_24h")
