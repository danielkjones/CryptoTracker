from test.helpers import example_listings_api_return, example_metadata_api_return
from unittest.mock import patch

import pytest

from src.api.coin_market_cap_api import CoinMarketCapApi

# NOTE: Test Constants and helpers live in test.helpers to
#  avoid repeat work


class TestCoinMarketCapApi:

    @patch(
        "src.api.coin_market_cap_api.CoinMarketCapApi.get_latest_listings",
        return_value=example_listings_api_return(),
    )
    def test_get_all_listings(self, mock_get_latest_listings):
        api = CoinMarketCapApi()
        listings = api.get_all_latest_listings()
        # this is imperfect since we are mocking, but need to make sure we are paginating
        assert len(listings) >= 10648

    @patch(
        "src.api.coin_market_cap_api.CoinMarketCapApi.get_metadata",
        return_value=example_metadata_api_return(),
    )
    def test_get_metadata_safe(self, mock_get_metadata):
        api = CoinMarketCapApi()
        fake_symbols = []
        for i in range(500):
            fake_symbols.append(f"BTC{i}")

        metadata_objs = api.get_metadata_safe(fake_symbols)

        # The fake response has 4 objects. We make 1 api call for every 240 ids.
        # There should be 3 calls made for 500 ids.
        # Total list should have 4 x 3 = 12 objects
        assert len(metadata_objs) == 12
