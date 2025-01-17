from os.path import dirname, join
from typing import Dict, List

import pandas as pd

from src.api.coin_market_cap_api import CoinMarketCapApi


class UniverseStep:

    def __init__(self):
        self.universe_file_format = "crypto_universe_{}.csv"
        # Same as ../../data_lake/universe
        self.universe_base_path = join(
            dirname(dirname(dirname(__file__))), "data_lake/universe"
        )

    # TODO do this a more data engineering way. We have the dataset with all the
    # information already. We should be able to use the data locations as the inputs

    def generate_universe(self, symbols: List[str], timestamp: str) -> pd.DataFrame:
        """For a list of Tickers, gather the metadata from the upstream API,
        save into the data lake, and return the dataframe for use in other
        workflow steps

        TODO what is the max number of tickers that this can operate with?
        may need to split up the calls into a more reasonable amount if the
        list is large (ie 10,000+)

        Args:
            symbols (List[str]): List of Crypto Symbols to gather metadata on
            timestamp (str): UTC Timestamp in YYYYMMDDHHMMSS to use for file naming

        Returns:
            pd.DataFrame: DataFrame containing the universe of Crypto Metadata
        """
        # Call the upstream Metadata API to gather the information.
        metadata = self.get_metadata(symbols)
        # Build out a pd dataframe
        df = pd.DataFrame(metadata)
        # write the dataframe to .csv in datalake
        self.write_dataframe(df, timestamp)
        # return the dataframe to be used by other workflow steps
        return df

    def get_metadata(self, symbols: List[str]) -> List[Dict]:
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
        unique_tickers = set(symbols)
        metadata = api.get_metadata(",".join(unique_tickers))
        return metadata

    def write_dataframe(self, df: pd.DataFrame, timestamp: str) -> None:
        """Write the dataframe to the datalake, with the properly formatted
        file format

        TODO Confirm that this is the right .csv output

        Args:
            df (pd.DataFrame): Dataframe holding all metadata
            timestamp (str): UTC timestamp in YYYYMMDDHHMMSS format
        """
        dataset_path = join(
            self.universe_base_path, self.universe_file_format.format(timestamp)
        )
        df.to_csv(dataset_path)
