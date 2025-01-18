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
        if not self.dataset_exists():
            crypto_ids = self.read_listings_ids()
            # Call the upstream Metadata API to gather the information.
            metadata = self.get_metadata(crypto_ids)
            # Build out a pd dataframe
            df = pd.DataFrame(metadata)
            # write the dataframe to .csv in datalake
            self.write_dataframe(df)
            # return the dataframe to be used by other workflow steps
            return df

    def read_listings_ids(self) -> List[str]:
        # TODO error handling on file not exists
        df = pd.read_csv(
            join(
                self.listings_base_path,
                self.listings_file_format.format(self.timestamp),
            )
        )
        # get list of symbols that is deduplicated
        ids = list(df["id"])
        return ids

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

    def dataset_exists(self) -> bool:
        """Check to see if the file already exists. If it does
        we should use that file instead of recreating.

        Returns:
            bool: True if exists, False otherwise
        """
        dataset_path = join(
            self.universe_base_path, self.universe_file_format.format(self.timestamp)
        )
        return exists(dataset_path)

    def write_dataframe(self, df: pd.DataFrame) -> None:
        """Write the dataframe to the datalake, with the properly formatted
        file format

        Args:
            df (pd.DataFrame): Dataframe holding all metadata
            timestamp (str): UTC timestamp in YYYYMMDDHHMMSS format
        """
        dataset_path = join(
            self.universe_base_path, self.universe_file_format.format(self.timestamp)
        )
        df.to_csv(dataset_path)
