import json
import os
from datetime import datetime, timezone
from os.path import dirname, join
from typing import Dict
from unittest.mock import patch

import pytest
from pandas import DataFrame

from src.steps.listings import ListingsStep
from src.util.constants import TIMESTAMP_FORMAT

# TODO move this to a shared function somewhere instead of repeating

TEST_DATA_LAKE_LOCATION = join(dirname(dirname(__file__)), "temp_data_lake/listings")


def example_listings_return() -> Dict:
    """Retrieve the example return from the listings call.

    Returns:
        Dict: JSON object that would be returned from the upstream API
    """
    curr_dir = dirname(__file__)
    # Traversing up directories using dirname()
    full_path = join(
        dirname(dirname(curr_dir)), "examples/listings/example_listings_all_res.json"
    )
    with open(full_path, "r") as file:
        jsn = json.load(file)
        return jsn


def delete_directory_contents(directory: str) -> None:
    """Delete the contents of a given directory

    Args:
        directory (str): Directory to delete files from
    """
    for filename in os.listdir(directory):
        file_path = os.path.join(directory, filename)
        if os.path.isfile(file_path):
            os.remove(file_path)


@pytest.fixture
def clean_test_directory():
    delete_directory_contents(TEST_DATA_LAKE_LOCATION)
    yield
    delete_directory_contents(TEST_DATA_LAKE_LOCATION)


class TestListings:

    def test_listings_base_path(self):
        # Generating a test here to confirm that this is the right pattern
        path = ListingsStep().listings_base_path
        expected_path = join(dirname(dirname(dirname(__file__))), "data_lake/listings")
        assert path == expected_path

    @patch(
        "src.api.coin_market_cap_api.CoinMarketCapApi.get_latest_listings",
        return_value=example_listings_return(),
    )
    def test_generate_listings(self, mock_get_latest_listings, clean_test_directory):
        listings_step = ListingsStep()
        # overwrite the output location for the sake of testing
        listings_step.listings_base_path = TEST_DATA_LAKE_LOCATION
        # going to make sure that all datetimestamps are in UTC
        timestamp = "20250116000000"
        df = listings_step.generate_listings(timestamp=timestamp)

        assert len(df) == 15000
        assert isinstance(df, DataFrame)
        assert len(os.listdir(TEST_DATA_LAKE_LOCATION)) == 1
