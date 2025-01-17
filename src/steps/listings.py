from os.path import dirname, join
from typing import Dict, List

import pandas as pd

from src.api.coin_market_cap_api import CoinMarketCapApi


class ListingsStep:

    def __init__(self):
        self.listings_file_format = "crypto_listings_{}.csv"
        # same as ../../data_lake/listings
        self.listings_base_path = join(dirname(dirname(dirname(dirname(__file__)))))

    def generate_listings(self, timestamp: str) -> pd.DataFrame:
        """Gets the listings from the upstream CoinMarketCapAPI, writes to datalake location
        as a unique .csv, and returns the DataFrame for use in other workflow steps

        Args:
            timestamp (str): UTC Timestamp in the format YYYYMMDDHHMMSS for uniqueness

        Returns:
            pd.DataFrame: Pandas DataFrame with the Crpto listings
        """
        listings = self.fetch_listings_upstream()
        # load listings into a dataframe
        df = pd.DataFrame(listings)
        # write the listings to the output location with a timestamp
        self.write_dataframe(df=df, timestamp=timestamp)
        # return the dataframe that holds the listings so we can re-use
        return df

    def fetch_listings_upstream(self) -> List[Dict]:
        """External call to the CoinMarketCapAPI to gather all listings

        Returns:
            List[Dict]: List of Coin Data
        """
        # Fetch listings from the upstream
        api = CoinMarketCapApi()
        listings = api.get_all_latest_listings()
        return listings

    def write_dataframe(self, df: pd.DataFrame, timestamp: str) -> None:
        """Write the dataframe to the output datalake with the properly
        timestamp formatting naming

        TODO Confirm that that this is the proper format for the CSV

        Args:
            df (pd.DataFrame): DataFrame to write
            timestamp (str): UTC Timestamp in YYYYMMDDHHMMSS format
        """
        dataset_path = join(
            self.listings_base_path, self.listings_file_format.format(timestamp)
        )
        df.to_csv(dataset_path)
