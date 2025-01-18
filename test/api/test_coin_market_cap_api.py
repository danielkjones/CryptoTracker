import json
from os.path import dirname, join
from test.helpers import example_listings_api_return
from typing import Dict
from unittest.mock import patch

import pytest
from dotenv import load_dotenv

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

    # TESTS BELOW HIT LIVE API ENVIRONMENT -
    #
    # Uncomment only if you configure .env for sandbox
    # environment OR you are willing to hit API on your account.
    #
    # Leaving in case interaction with API needs further live testing.
    #
    # @pytest.mark.api_test
    # def test_get_metadata(self):
    #     api = CoinMarketCapApi()
    #     metadata = api.get_metadata("BTC,ETH,DOGE")

    #     # Potentially a flaky test depending on how the API responds
    #     assert len(metadata) == 3
    #     assert metadata[0]["symbol"] == "BTC"
    #     assert metadata[1]["symbol"] == "DOGE"
    #     assert metadata[2]["symbol"] == "ETH"
    #
    # @pytest.mark.api_test
    # def test_get_listings(self):
    #     api = CoinMarketCapApi()
    #     listings = api.get_latest_listings(start=1, limit=100)

    #     assert listings.get("status") is not None
    #     assert listings.get("data") is not None
