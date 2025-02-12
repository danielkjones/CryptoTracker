import logging
from datetime import datetime, timezone
from os.path import exists, join
from typing import Optional

import pandas as pd

from src.util.config import (
    COINS_TO_TRACK_CSV_NAME,
    COINS_TO_TRACK_DATA_LOCATION,
    LISTINGS_CSV_FORMAT,
    LISTINGS_DATA_LOCATION,
    LOGGER_NAME,
    PRICING_CSV_FORMAT,
    PRICING_DATA_LOCATION,
    TIMESTAMP_FORMAT,
)
from src.util.dataframe_ops import read_csv, write_csv
from src.util.exceptions import InvalidSymbolException

logger = logging.getLogger(LOGGER_NAME)


class PricingStep:

    def __init__(self, timestamp: str):
        """Step object that gathers pricing information from the listing
        data based on user inputted symbols to track. Normalizes naming to
        pascal case for all column names. Writes output to data lake as
        .csv.

        Args:
            timestamp (str): UTC Timestamp of execution in YYYYMMDDHHMMSS
        """
        self.timestamp = timestamp

        # input dataset details
        self.configuration_file_directory = COINS_TO_TRACK_DATA_LOCATION
        self.configuration_file_name = COINS_TO_TRACK_CSV_NAME

        self.listings_file_directory = LISTINGS_DATA_LOCATION
        self.listings_file_format = LISTINGS_CSV_FORMAT

        # output dataset details
        self.pricing_file_directory = PRICING_DATA_LOCATION
        self.pricing_file_format = PRICING_CSV_FORMAT

    @property
    def coins_to_track_csv(self) -> str:
        return join(self.configuration_file_directory, self.configuration_file_name)

    @property
    def listings_csv(self) -> str:
        return join(
            self.listings_file_directory,
            self.listings_file_format.format(self.timestamp),
        )

    @property
    def pricing_csv(self) -> str:
        return join(
            self.pricing_file_directory, self.pricing_file_format.format(self.timestamp)
        )

    @property
    def iso_timestamp(self) -> str:
        """Generate the ISO Timestamp from the given timestamp in
        YYYYMMHHMMSS format. Little hacky, but want timestamp consistency
        across all data generated internally and externally.

        Returns:
            str: Time stamp in ISO format - YYYY-MM-DDTHH:MM:SS.mmmZ
        """
        return f"{datetime.strptime(self.timestamp, TIMESTAMP_FORMAT).isoformat(
            timespec="milliseconds"
        )}Z"

    def generate_pricing(self) -> Optional[pd.DataFrame]:
        """Read in the coins to track configuration file and gather
        the pricing information on those coins from the coins list.

        If there are any invalid coins in the configuration file there
        will be an exception thrown and the user will be prompted to
        make adjustments.

        If there already exists a file for the given execution timestamp, skips step to
        avoid repeat work.

        Returns:
            Optional[pd.DataFrame]: DataFrame with cleaned pricing information for
                user inputted crypto symbols. None if file already existed.
        """
        if exists(self.pricing_csv):
            logger.info(
                f"Dataset already exists at '{self.pricing_csv}'. Using pre-existing dataset "
                "instead of generating. \n If you desire to generate a new dataset re-run without providing a timestamp."
            )
        else:
            coins_df = read_csv(self.coins_to_track_csv)
            listings_df = read_csv(self.listings_csv)

            # validate all the coins to track are legitimate
            self.validate_symbols(coins_df, listings_df)
            # if all coins are legitimate, select listings for coins based on symbol
            coins_listings_df = self.select_coins_to_track(coins_df, listings_df)
            # dedup symbols, grabbing only the top CMC ranked if multiple rows for a symbol
            deduped_listings_df = self.dedup_symbols(coins_listings_df)
            # select only columns we care about, rename columns to pascal for consistency
            pricing_df = self.select_pricing_data_columns(deduped_listings_df)
            # tag with LoadedWhen and IsTopCurrency
            enriched_pricing_df = self.enrich_pricing_dataframe(pricing_df)

            write_csv(self.pricing_csv, enriched_pricing_df)
            return enriched_pricing_df

    def validate_symbols(
        self, coins_df: pd.DataFrame, listings_df: pd.DataFrame
    ) -> None:
        """Run validation to confirm all user input coins to track
        are present in the crypto listings. If not, log and error and
        throw exception so user knows what to adjust.

        Args:
            coins_df (pd.DataFrame): Dataframe with list of coins to track
            listings_df (pd.DataFrame): Dataframe with all crypto listings

        Raises:
            InvalidSymbolException: Exception thrown when there is an invalid user input
        """
        # we should be able to read in the data
        # Check if all the symbols in the coins to track are valid coins
        invalid_df = coins_df[~coins_df["Symbol"].isin(listings_df["symbol"])]

        # Raise an error if there is an invalid symbol in the coins_to_track.csv
        if len(invalid_df) > 0:
            invalid_symbols = list(invalid_df["Symbol"])
            msg = f"Following Symbols are not valid Coin Symbols: {invalid_symbols}. Please remove from the coins_to_track.csv configuration and re-run process.\nYou may re-run and provide '--timestamp={self.timestamp}' argument to resume at this step."
            logger.error(msg)
            raise InvalidSymbolException(msg)

    def select_coins_to_track(
        self, coins_df: pd.DataFrame, listings_df: pd.DataFrame
    ) -> pd.DataFrame:
        """Gather listing data for only the coins to track

        Args:
            coins_df (pd.DataFrame): Dataframe with list of coins to track
            listings_df (pd.DataFrame): Dataframe with all crypto listings

        Returns:
            pd.DataFrame: Dataframe of listings for the given coins
        """
        coins_listings_df = pd.merge(
            coins_df, listings_df, how="left", left_on="Symbol", right_on="symbol"
        )
        return coins_listings_df

    def dedup_symbols(self, listings_df: pd.DataFrame) -> pd.DataFrame:
        """There are multiple listings for a singular Symbol. Only return
        the lowest CMC Ranked Symbol.

        Args:
            pricing_df (pd.DataFrame): Pricing DF with potential duplicates for a symbol

        Returns:
            pd.DataFrame: Deduped Pricing DF
        """
        listings_df["rank"] = listings_df.groupby("Symbol")["cmc_rank"].rank(
            method="first", ascending=True
        )
        deduped_pricing_df = listings_df[listings_df["rank"] == 1].drop(columns="rank")
        return deduped_pricing_df.reset_index(drop=True)

    def select_pricing_data_columns(self, listings_df: pd.DataFrame) -> pd.DataFrame:
        """Select all columns relevant to pricing data. Rename the columns to
        pascal case for column consistency / usability.

        Args:
            listings_df (pd.DataFrame): DataFrame with all Listing data

        Returns:
            pd.DataFrame: DataFrame with only pricing data, with columns renamed in pascal case
        """
        return listings_df[
            [
                "id",
                "name",
                "Symbol",
                "slug",
                "cmc_rank",
                "quote.USD.price",
                "quote.USD.volume_24h",
                "quote.USD.volume_change_24h",
                "quote.USD.percent_change_1h",
                "quote.USD.percent_change_24h",
                "quote.USD.percent_change_7d",
                "quote.USD.percent_change_30d",
                "quote.USD.percent_change_60d",
                "quote.USD.percent_change_90d",
                "quote.USD.market_cap",
                "quote.USD.market_cap_dominance",
                "quote.USD.fully_diluted_market_cap",
                "quote.USD.tvl",
                "quote.USD.last_updated",
            ]
        ].rename(
            columns={
                "id": "ID",
                "name": "Name",
                "slug": "Slug",
                "cmc_rank": "CMCRank",
                "quote.USD.price": "Price",
                "quote.USD.volume_24h": "Volume24h",
                "quote.USD.volume_change_24h": "VolumeChange24h",
                "quote.USD.percent_change_1h": "PercentChange1h",
                "quote.USD.percent_change_24h": "PercentChange24h",
                "quote.USD.percent_change_7d": "PercentChange7d",
                "quote.USD.percent_change_30d": "PercentChange30d",
                "quote.USD.percent_change_60d": "PercentChange60d",
                "quote.USD.percent_change_90d": "PercentChange90d",
                "quote.USD.market_cap": "MarketCap",
                "quote.USD.market_cap_dominance": "MarketCapDominance",
                "quote.USD.fully_diluted_market_cap": "FullyDilutedMarketCap",
                "quote.USD.tvl": "TVL",
                "quote.USD.last_updated": "LastUpdated",
            }
        )

    def enrich_pricing_dataframe(self, pricing_df: pd.DataFrame) -> pd.DataFrame:
        """Append 'LoadedWhen' timestamp field and 'IsTopCurrency' boolean.

        Args:
            pricing_df (pd.DataFrame): Cleaned pricing dataframe in pascal case

        Returns:
            pd.DataFrame: enriched dataframe with extra fields
        """
        pricing_df["LoadedWhen"] = self.iso_timestamp
        pricing_df["IsTopCurrency"] = pricing_df["CMCRank"] <= 10
        return pricing_df
