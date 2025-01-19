import logging
from os.path import exists, join
from typing import Dict, List, Optional

import pandas as pd

from src.api.coin_market_cap_api import CoinMarketCapApi
from src.util.config import (
    LISTINGS_CSV_FORMAT,
    LISTINGS_DATA_LOCATION,
    LOGGER_NAME,
    UNIVERSE_CSV_FORMAT,
    UNIVERSE_DATA_LOCATION,
)
from src.util.dataframe_ops import read_csv, write_csv

logger = logging.getLogger(LOGGER_NAME)


class UniverseStep:

    def __init__(self, timestamp: str):
        """Step object that calls the CoinMarketCap API to pull the static Metadata
        for all coins in the list of active cryptocurrencies and saves the raw data
        to the data lake as a .csv.

        Args:
            timestamp (str): UTC Timestamp of execution in YYYYMMDDHHMMSS
        """
        self.timestamp = timestamp

        # input dataset details
        self.listings_base_path = LISTINGS_DATA_LOCATION
        self.listings_file_format = LISTINGS_CSV_FORMAT

        # output dataset details
        self.universe_base_path = UNIVERSE_DATA_LOCATION
        self.universe_file_format = UNIVERSE_CSV_FORMAT

    @property
    def listings_csv(self) -> str:
        return join(
            self.listings_base_path,
            self.listings_file_format.format(self.timestamp),
        )

    @property
    def universe_csv(self) -> str:
        return join(
            self.universe_base_path, self.universe_file_format.format(self.timestamp)
        )

    def generate_universe(self) -> Optional[pd.DataFrame]:
        """For a list of Tickers, gather the metadata from the upstream API,
        save into the data lake, and return the dataframe.

        If there already exists a file for the given execution timestamp, skips step to
        avoid repeat work.

        Returns:
            Optional[pd.DataFrame]: DataFrame containing the universe of crypto metadata.
                None if file already existed.
        """
        if exists(self.universe_csv):
            logger.info(
                f"Dataset already exists at '{self.universe_csv}'. Using pre-existing dataset instead of generating new dataset. \nIf you desire to generate a new dataset re-run without providing a timestamp."
            )
        else:
            listings_df = read_csv(self.listings_csv)
            crypto_ids = list(listings_df["id"])
            # Call the upstream Metadata API to gather the information.
            metadata = self.get_metadata(crypto_ids)
            # Build out a flattened dataframe
            df = pd.json_normalize(metadata)
            # write the dataframe to .csv in datalake
            write_csv(self.universe_csv, df)
            # return the dataframe to be used by other workflow steps
            return df

    def get_metadata(self, ids: List[str]) -> List[Dict]:
        """Get Metadata for a given list of CMC Coin IDs.

        Args:
            ids (List[str]): List of CMC Coin IDs to gather metadata on

        Raises:
            e: Exception from connecting with the Metadata API

        Returns:
            List[Dict]: List of metadata objects for each CMC Coin ID present
        """
        try:
            api = CoinMarketCapApi()
            metadata = api.get_metadata_safe(ids)
            return metadata
        except Exception as e:
            logger.error(
                "ERROR getting metadata from CMC API to generate universe dataset. Investigate connection, authentication, and inputs, and try re-running process."
            )
            raise e
