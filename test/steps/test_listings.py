import json
import os
from datetime import datetime, timezone
from os.path import dirname, join
from test.helpers import TestConstants as tc
from test.helpers import delete_directory_contents, example_listings_api_return
from typing import Dict
from unittest.mock import patch

import pytest
from pandas import DataFrame

from src.steps.listings import ListingsStep
from src.util.constants import TIMESTAMP_FORMAT

# NOTE: Test Constants and helpers live in test.helpers to
#  avoid repeat work


@pytest.fixture
def clean_test_directory():
    """Test writes out to Temp Listings Directory. Need to clean up
    before and after tests.
    """
    delete_directory_contents(tc.TEMP_LISTINGS_DIRECTORY)
    yield
    delete_directory_contents(tc.TEMP_LISTINGS_DIRECTORY)


class TestListings:

    def test_listings_base_path(self):
        path = ListingsStep(tc.TEST_TIMESTAMP).listings_base_path
        expected_path = join(dirname(dirname(dirname(__file__))), "data_lake/listings")
        assert path == expected_path

    @patch(
        "src.api.coin_market_cap_api.CoinMarketCapApi.get_latest_listings",
        return_value=example_listings_api_return(),
    )
    def test_generate_listings(self, mock_get_latest_listings, clean_test_directory):
        listings_step = ListingsStep(tc.TEST_TIMESTAMP)
        # Overwriting data location properties to use predictable test locations
        listings_step.listings_base_path = tc.TEMP_LISTINGS_DIRECTORY
        df = listings_step.generate_listings()

        assert len(df) == 15000
        assert isinstance(df, DataFrame)
        assert (
            len(os.listdir(tc.TEMP_LISTINGS_DIRECTORY)) == 1
        ), "Test data should have been written out to data lake location"
