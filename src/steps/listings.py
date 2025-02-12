import logging
from os.path import exists, join
from typing import Dict, List, Optional

import pandas as pd

from src.api.coin_market_cap_api import CoinMarketCapApi
from src.util.config import LISTINGS_CSV_FORMAT, LISTINGS_DATA_LOCATION, LOGGER_NAME
from src.util.dataframe_ops import write_csv

logger = logging.getLogger(LOGGER_NAME)


class ListingsStep:

    def __init__(self, timestamp: str):
        """Step object that calls the CoinMarketCap API to pull latest listings of
        active cryptocurrencies and saves raw data to data lake as .csv.

        Args:
            timestamp (str): UTC Timestamp of execution in YYYYMMDDHHMMSS
        """
        self.timestamp = timestamp
        # output dataset details
        self.listings_base_path = LISTINGS_DATA_LOCATION
        self.listings_file_format = LISTINGS_CSV_FORMAT

    @property
    def listings_csv(self) -> str:
        return join(
            self.listings_base_path, self.listings_file_format.format(self.timestamp)
        )

    def generate_listings(self) -> Optional[pd.DataFrame]:
        """Gets the listings from the upstream CoinMarketCapAPI, writes to data lake location
        as a unique .csv, and returns the DataFrame for use in other workflow steps.

        If there already exists a file for the given execution timestamp, skips step to
        avoid repeat work.

        Args:
            timestamp (str): UTC Timestamp in the format YYYYMMDDHHMMSS for uniqueness

        Returns:
            Optional[pd.DataFrame]: DataFrame with the Crypto listings. None
                if file already exists.
        """
        if exists(self.listings_csv):
            logger.info(
                f"Dataset already exists at '{self.listings_csv}'. Using pre-existing dataset instead of generating new dataset. \nIf you desire to generate a new dataset re-run without providing a timestamp."
            )
        else:
            listings = self.fetch_listings_upstream()
            # Build out a flattened dataframe
            df = pd.json_normalize(listings)
            write_csv(self.listings_csv, df)
            return df

    def fetch_listings_upstream(self) -> List[Dict]:
        """External call to the CMC API to gather all crypto coin listings.

        Raises:
            e: Exception from connecting with the Listings API

        Returns:
            List[Dict]: List of Coin Data
        """
        try:
            api = CoinMarketCapApi()
            listings = api.get_all_latest_listings()
            return listings
        except Exception as e:
            logger.error(
                "ERROR getting crypto listings from CMC API to generate listings dataset. Investigate connection, authentication, and inputs, and try re-running process."
            )
            raise e
