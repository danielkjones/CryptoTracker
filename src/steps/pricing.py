from os.path import dirname, join
from typing import List

import pandas as pd

from src.util.exceptions import InvalidSymbolException


class PricingStep:

    def __init__(self, timestamp: str):
        self.timestamp = timestamp

        # TODO seeing a pattern here, these should be moved into constants

        self.configuration_file_directory = join(
            dirname(dirname(dirname(__file__))), "data_lake/configuration"
        )
        self.configuration_file_name = "coins_to_track.csv"

        self.listings_file_directory = join(
            dirname(dirname(dirname(__file__))), "data_lake/listings"
        )
        self.listings_file_format = "crypto_listings_{}.csv"

        self.pricing_file_directory = join(
            dirname(dirname(dirname(__file__))), "data_lake/pricing"
        )
        self.pricing_file_format = "coins_pricing_{}.csv"

    def generate_pricing(self) -> pd.DataFrame:
        coins_df = self.read_coins_to_track()
        listings_df = self.read_listings()
        self.validate_symbols(coins_df=coins_df, listings_df=listings_df)
        pricing_df = self.enhance_coins_to_track(coins_df, listings_df)
        deduped_pricing_df = self.dedup_symbols(pricing_df)
        self.write_dataframe(deduped_pricing_df)
        return deduped_pricing_df

    def read_coins_to_track(self) -> pd.DataFrame:
        # Read in the CSV from the configuration location (it is a dataset)
        df = pd.read_csv(
            join(self.configuration_file_directory, self.configuration_file_name)
        )
        return df

    def read_listings(self) -> pd.DataFrame:
        df = pd.read_csv(
            join(
                self.listings_file_directory,
                self.listings_file_format.format(self.timestamp),
            )
        )
        return df

    def validate_symbols(
        self, coins_df: pd.DataFrame, listings_df: pd.DataFrame
    ) -> None:
        # we should be able to read in the data
        # Check if all the symbols in the coins to track are valid coins
        invalid_df = coins_df[~coins_df["Symbol"].isin(listings_df["symbol"])]

        # Raise an error if there is an invalid symbol in the coins_to_track.csv
        if len(invalid_df) > 0:
            invalid_symbols = list(invalid_df["Symbol"])
            msg = f"Following Symbols are not valid Coin Symbols: {invalid_symbols}. Please remove from the coins_to_track.csv configuration and re-run process."
            raise InvalidSymbolException(msg)

    def enhance_coins_to_track(
        self, coins_df: pd.DataFrame, listings_df: pd.DataFrame
    ) -> pd.DataFrame:
        pricing_df = pd.merge(
            coins_df, listings_df, how="left", left_on="Symbol", right_on="symbol"
        )
        # TODO may want to use a real timestamp, not the fake format that we are using
        pricing_df["LoadedWhen"] = self.timestamp
        pricing_df["IsTopCurrency"] = pricing_df["cmc_rank"] <= 10
        return pricing_df

    def dedup_symbols(self, pricing_df: pd.DataFrame) -> pd.DataFrame:
        """There are multiple listings for a singular Symbol. Only return
        the lowest CMC Ranked Symbol.

        Args:
            pricing_df (pd.DataFrame): Pricing DF with potential duplicates for a symbol

        Returns:
            pd.DataFrame: Deduped Pricing DF
        """
        pricing_df["rank"] = pricing_df.groupby("Symbol")["cmc_rank"].rank(
            method="first", ascending=True
        )
        deduped_pricing_df = pricing_df[pricing_df["rank"] == 1].drop(columns="rank")
        return deduped_pricing_df.reset_index(drop=True)

    def write_dataframe(self, pricing_df: pd.DataFrame) -> None:
        dataset_path = join(
            self.pricing_file_directory, self.pricing_file_format.format(self.timestamp)
        )
        pricing_df.to_csv(dataset_path)
