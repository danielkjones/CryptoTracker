import json
import os
from datetime import datetime, timezone
from os.path import dirname, join
from typing import Dict, List
from unittest.mock import patch

import pytest

from src.steps.universe import UniverseStep
from src.util.constants import TIMESTAMP_FORMAT

TEST_DATA_LAKE_LOCATION = join(dirname(dirname(__file__)), "test_data_lake/listings")


def example_metadata_return() -> List[Dict]:
    """Retrieve the example return from the listings call.

    Returns:
        Dict: JSON object that would be returned from the upstream API
    """
    curr_dir = dirname(__file__)
    # Traversing up directories using dirname()
    full_path = join(
        dirname(dirname(curr_dir)),
        "examples/metadata/success/example_metadata_v1_res.json",
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

    @patch(
        "src.api.coin_market_cap_api.CoinMarketCapApi.get_metadata",
        return_value=example_metadata_return(),
    )
    def test_generate_universe(self, mock_get_metadata, clean_test_directory):
        example_tickers = ["BTC", "DOGE", "ETH", "SOL"]
        universe_step = UniverseStep()
        # Overwriting for testing purposes
        universe_step.universe_base_path = TEST_DATA_LAKE_LOCATION
        # Generate UTC timestamp for testing
        timestamp = datetime.now(timezone.utc).strftime(TIMESTAMP_FORMAT)
        df = universe_step.generate_universe(example_tickers, timestamp)

        assert len(df) == 4
        assert len(os.listdir(TEST_DATA_LAKE_LOCATION)) == 1
