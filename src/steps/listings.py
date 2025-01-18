from os.path import join
from typing import Dict, List

import pandas as pd

from src.api.coin_market_cap_api import CoinMarketCapApi
from src.util.config import LISTINGS_CSV_FORMAT, LISTINGS_DATA_LOCATION


class ListingsStep:

    def __init__(self, timestamp: str):
        self.timestamp = timestamp
        # output dataset details
        self.listings_base_path = LISTINGS_DATA_LOCATION
        self.listings_file_format = LISTINGS_CSV_FORMAT

    def generate_listings(self) -> pd.DataFrame:
        """Gets the listings from the upstream CoinMarketCapAPI, writes to data lake location
        as a unique .csv, and returns the DataFrame for use in other workflow steps

        Args:
            timestamp (str): UTC Timestamp in the format YYYYMMDDHHMMSS for uniqueness

        Returns:
            pd.DataFrame: Pandas DataFrame with the Crypto listings
        """
        listings = self.fetch_listings_upstream()
        df = pd.DataFrame(listings)
        self.write_dataframe(df=df)
        return df

    def fetch_listings_upstream(self) -> List[Dict]:
        """External call to the CoinMarketCapAPI to gather all listings

        Returns:
            List[Dict]: List of Coin Data
        """
        api = CoinMarketCapApi()
        listings = api.get_all_latest_listings()
        return listings

    def write_dataframe(self, df: pd.DataFrame) -> None:
        """Write the dataframe to the output data lake with the properly
        timestamp formatting naming

        TODO Confirm that that this is the proper format for the CSV

        Args:
            df (pd.DataFrame): DataFrame to write
            timestamp (str): UTC Timestamp in YYYYMMDDHHMMSS format
        """
        dataset_path = join(
            self.listings_base_path, self.listings_file_format.format(self.timestamp)
        )
        df.to_csv(dataset_path)
