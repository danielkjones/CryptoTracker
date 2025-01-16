import os
from typing import Dict, List
from urllib.parse import urljoin

import requests


class CoinMarketCapApi:
    """Coin Market Cap API

    Docs: https://coinmarketcap.com/api/documentation/v1/#section/Introduction
    """

    def __init__(self):
        self.host = os.getenv("COIN_MARKET_CAP_HOST")
        self.access_token = os.getenv("COIN_MARKET_CAP_ACCESS_KEY")

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
        return res.json()

    def get_all_latest_listings(self) -> List[str]:
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

    def get_metadata(self, symbols: str) -> Dict:
        """Get Metadata on a singular or collection of CryptoCurrencies.

        TODO need to switch to using some sort of error handling on this

        Reference: https://coinmarketcap.com/api/documentation/v1/#operation/getV1CryptocurrencyInfo

        Args:
            symbols (str): Comma separated value of Cypto Tickers to gather data on

        Returns:
            Dict: Metadata on the
        """
        url = urljoin(self.host, "/v1/cryptocurrency/info")
        params = {"symbol": symbols}
        res = requests.get(url=url, params=params, headers=self.headers)
        return res.json()
