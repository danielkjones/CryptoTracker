import json
import os
from datetime import datetime, timezone
from os.path import dirname, join
from typing import Dict, List
from unittest.mock import patch

import pytest

from src.steps.universe import UniverseStep
from src.util.constants import TIMESTAMP_FORMAT

TEST_TIMESTAMP = "20250116000000"
TEST_DATA_LAKE_LOCATION = join(dirname(dirname(__file__)), "temp_data_lake/universe")
TEST_LISTINGS_LOCATION = join(dirname(dirname(__file__)), "mock_data_lake/listings")


def example_metadata_return() -> List[Dict]:
    """Retrieve the example return from the listings call.

    Returns:
        Dict: JSON object that would be returned from the upstream API
    """
    # Traversing up directories using dirname()
    full_path = join(
        dirname(dirname(__file__)),
        "mock_api_response/example_metadata_v1_res.json",
    )
    with open(full_path, "r") as file:
        jsn = json.load(file)
        return list(jsn["data"].values())


def delete_directory_contents(directory: str) -> None:
    """Delete the contents of a given directory
    TODO should probably move this to be a test helper that can be used in multiple locations

    Args:
        directory (str): Directory to delete files from
    """
    for filename in os.listdir(directory):
        file_path = os.path.join(directory, filename)
        if os.path.isfile(file_path):
            os.remove(file_path)


# TODO could probably consolidate all cleanup / teardown. We'll see
@pytest.fixture
def clean_test_directory():
    delete_directory_contents(TEST_DATA_LAKE_LOCATION)
    yield
    delete_directory_contents(TEST_DATA_LAKE_LOCATION)


class TestUniverse:

    def test_universe_base_path(self):
        # Generating a test here to confirm that this is the right pattern
        path = UniverseStep(TEST_TIMESTAMP).universe_base_path
        expected_path = join(dirname(dirname(dirname(__file__))), "data_lake/universe")
        assert path == expected_path

    @patch(
        "src.api.coin_market_cap_api.CoinMarketCapApi.get_metadata",
        return_value=example_metadata_return(),
    )
    def test_generate_universe(self, mock_get_metadata, clean_test_directory):
        universe_step = UniverseStep(TEST_TIMESTAMP)
        # Overwriting for testing purposes
        universe_step.universe_base_path = TEST_DATA_LAKE_LOCATION
        universe_step.listings_base_path = TEST_LISTINGS_LOCATION
        df = universe_step.generate_universe()

        assert len(df) == 4
        assert (
            len(os.listdir(TEST_DATA_LAKE_LOCATION)) == 1
        ), "Test data should have been written out to the data lake location"
