import json
from os.path import dirname, join
from typing import Dict
from unittest.mock import patch

import pytest
from dotenv import load_dotenv

from src.coin_market_cap_api import CoinMarketCapApi

# TODO This particular class is interacting with the actual API right now
# which is not a unit test, would need to change


def example_listings_return() -> Dict:
    """Retrieve the example return from the listings call.

    Returns:
        Dict: JSON object that would be returned from the upstream API
    """
    curr_dir = dirname(__file__)
    # Traversing up a directory using dirname()
    full_path = join(
        dirname(curr_dir), "examples/listings/example_listings_all_res.json"
    )
    with open(full_path, "r") as file:
        jsn = json.load(file)
        return jsn


@pytest.fixture(autouse=True, scope="module")
def load_env():
    """Load in the environment variables for any live tests"""
    curr_dir = dirname(__file__)
    env_path = join(dirname(curr_dir), ".env")
    load_dotenv(dotenv_path=env_path)


class TestCoinMarketCapApi:

    def test_get_headers(self):
        api = CoinMarketCapApi()
        headers = api.headers

        assert headers["Accepts"] == "application/json"
        assert headers["X-CMC_PRO_API_KEY"] == "b54bcf4d-1bca-4e8e-9a24-22ff2c3d462c"

    @pytest.mark.api_test
    def test_get_listings(self):
        api = CoinMarketCapApi()
        listings = api.get_latest_listings(start=1, limit=100)

        assert listings.get("status") is not None
        assert listings.get("data") is not None

    @patch(
        "src.coin_market_cap_api.CoinMarketCapApi.get_latest_listings",
        return_value=example_listings_return(),
    )
    def test_get_all_listings(self, mock_get_latest_listings):
        api = CoinMarketCapApi()
        listings = api.get_all_latest_listings()
        # this is imperfect since we are mocking, but need to make sure we are paginating
        assert len(listings) >= 10648

    @pytest.mark.api_test
    def test_get_metadata(self):
        api = CoinMarketCapApi()
        metadata = api.get_metadata("BTC,ETH,DOGE")

        assert metadata.get("status") is not None
        assert metadata.get("data") is not None
        assert "BTC" in metadata.get("data")
        assert "DOGE" in metadata.get("data")
        assert "ETH" in metadata.get("data")
