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
        for i in range(152):
            fake_symbols.append(f"BTC{i}")

        metadata_objs = api.get_metadata_safe(fake_symbols)

        # The fake response has 4 objects. We expect 3 API calls.
        # Total list should have 4 x 3 = 12 objects
        assert len(metadata_objs) == 12

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
