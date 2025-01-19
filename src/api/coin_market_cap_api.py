import os
from typing import Dict, List
from urllib.parse import quote_plus, urljoin

import requests
from retry import retry

from src.util.config import COIN_MARKET_CAP_ACCESS_KEY, COIN_MARKET_CAP_HOST


class CoinMarketCapApi:
    """Coin Market Cap API

    Docs: https://coinmarketcap.com/api/documentation/v1/#section/Introduction
    """

    def __init__(self):
        self.host = COIN_MARKET_CAP_HOST
        self.access_token = COIN_MARKET_CAP_ACCESS_KEY

    @property
    def headers(self) -> Dict:
        """Get the headers required for authentication

        Returns:
            Dict: Required Dict for Headers
        """
        return {
            "Accepts": "application/json",
            "X-CMC_PRO_API_KEY": self.access_token,
        }

    @retry(tries=3, delay=2, backoff=2)
    def get_latest_listings(self, start: int, limit: int) -> Dict:
        """
        Call to get the latest Listings. This includes all the data for a singular page.

        Reference: https://coinmarketcap.com/api/documentation/v1/#operation/getV1CryptocurrencyListingsLatest

        TODO Need retries and error handling

        Args:
            start (int): 1-indexed start index to retrieve from the list
            limit (int): Number of listings to return (1-5000)

        Returns:
            Dict: Dictionary with "data" and "status" objects

        """
        url = urljoin(self.host, "/v1/cryptocurrency/listings/latest")
        parameters = {
            "start": str(start),
            "limit": str(limit),
            "convert": "USD",
            "sort": "market_cap",
        }
        res = requests.get(url, headers=self.headers, params=parameters)
        # use requests library to raise exceptions based on status
        res.raise_for_status()
        return res.json()

    def get_all_latest_listings(self) -> List[Dict]:
        """Paginate through all the latest listings and return a list of the
        available CryptoCurrency listings

        TODO need to test this against live data, but this is expensive so holding
        off for now

        Returns:
            List[Dict]: Returns Dict information on all the available listings
        """
        listings = []
        start = 1
        limit = 5000
        total_count = None

        while total_count is None or len(listings) < total_count:
            res = self.get_latest_listings(start, limit)
            if not total_count:
                # To avoid an infinite loop, based off of sandbox this may not
                # return if there are only a few listings
                total_count = res["status"].get("total_count", 0)
            listings.extend(res["data"])
            # reset the start for the next loop
            start = len(listings) + 1

        return listings

    def get_metadata_safe(self, ids: List[str]) -> List[Dict]:
        """Due to the constraints of URI length, requests for metadata may
        need to be broken up into multiple requests.

        Break up the ids into batches before making requests for metadata.

        Returns:
            List[Dict]: Metadata "data" objects from the API response
        """
        # Logic behind batch size:
        # - generally accepted "max_length" for URI = 2000 chars
        # - "host_length" (with "?id=") = 60 chars
        # - current "largest_id_size" = 5 chars
        # - "buffer" for each ID (for growth if IDs get large) = 2 chars
        # - "comma" for each ID = 1 char
        #
        # max_batch_size = (max_length - host_length) / (largest_id_size + buffer + comma)
        # max_batch_size = (2000 - 60) / (5 + 2 + 1 ) = 242.5
        batch_size = 240

        # Breaking up the entire list of symbols into batches no greater than 100 symbols long
        id_batches = [ids[i : i + batch_size] for i in range(0, len(ids), batch_size)]

        # Create a list of the metadata objects from separate API calls
        metadata_objs = []
        for id_batch in id_batches:
            metadata_objs.extend(self.get_metadata(id_batch))

        return metadata_objs

    @retry(tries=7, delay=3, backoff=5)
    def get_metadata(self, ids: List[str]) -> List[Dict]:
        """Get Metadata on a singular or collection of CryptoCurrencies using CMC IDs.

        NOTE: Using a large number of retries, delay, and backoff since this API has caused
            multiple 429 Client errors. May have room for optimization.

        Reference: https://coinmarketcap.com/api/documentation/v1/#operation/getV1CryptocurrencyInfo

        Args:
            symbols (str): Comma separated value of crypto CMC IDs to gather data on

        Returns:
            Dict: Metadata "data" object from the API response
        """
        url = urljoin(self.host, "/v1/cryptocurrency/info")
        # convert to a list of strings and join as comma separated values
        ids_str = ",".join(map(str, ids))
        params = {"id": ids_str}
        res = requests.get(url=url, params=params, headers=self.headers)
        # use requests library to raise exceptions based on status
        res.raise_for_status()
        metadata_obj = res.json()["data"]
        return list(metadata_obj.values())
