from os.path import exists, join
from typing import Dict, List, Optional

import pandas as pd

from src.api.coin_market_cap_api import CoinMarketCapApi
from src.util.config import (
    LISTINGS_CSV_FORMAT,
    LISTINGS_DATA_LOCATION,
    UNIVERSE_CSV_FORMAT,
    UNIVERSE_DATA_LOCATION,
)


class UniverseStep:

    def __init__(self, timestamp: str):
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
        save into the data lake, and return the dataframe for use in other
        workflow steps

        Args:
            symbols (List[str]): List of Crypto Symbols to gather metadata on
            timestamp (str): UTC Timestamp in YYYYMMDDHHMMSS to use for file naming

        Returns:
            Optional[pd.DataFrame]: DataFrame containing the universe
                of Crypto Metadata if generated by this process
        """
        if not exists(self.universe_csv):
            listings_df = pd.read_csv(self.listings_csv)
            crypto_ids = list(listings_df["id"])
            # Call the upstream Metadata API to gather the information.
            metadata = self.get_metadata(crypto_ids)
            # Build out a flattened dataframe
            df = pd.json_normalize(metadata)
            # write the dataframe to .csv in datalake
            df.to_csv(self.universe_csv, index=False)
            # return the dataframe to be used by other workflow steps
            return df

    def get_metadata(self, ids: List[str]) -> List[Dict]:
        """Get Metadata for a given list of tickers.

        Will ensure uniqueness to avoid gathering extra data.

        Args:
            symbols (List[str]): List of Crypto Symbols to gather metadata on

        Returns:
            Dict: Dict of Symbols with Metadata in the format
                {
                    "Symbol": {
                        {{ metadata key / value pairs }}
                    }
                }
        """
        api = CoinMarketCapApi()
        metadata = api.get_metadata_safe(ids)
        return metadata
