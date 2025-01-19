import os
from os.path import dirname, join
from test.helpers import TestConstants as tc
from test.helpers import delete_directory_contents, example_metadata_api_return
from unittest.mock import patch

import pytest

from src.steps.universe import UniverseStep

# NOTE: Test Constants and helpers live in test.helpers to
#  avoid repeat work


@pytest.fixture
def clean_test_directory():
    """Test writes out to Temp Universe Directory. Need to clean up
    before and after tests.
    """
    delete_directory_contents(tc.TEMP_UNIVERSE_DIRECTORY)
    yield
    delete_directory_contents(tc.TEMP_UNIVERSE_DIRECTORY)


class TestUniverse:

    def test_universe_base_path(self):
        # Generating a test here to confirm that this is the right pattern
        path = UniverseStep(tc.TEST_TIMESTAMP).universe_base_path
        expected_path = join(dirname(dirname(dirname(__file__))), "data_lake/universe")
        assert path == expected_path

    @patch(
        "src.api.coin_market_cap_api.CoinMarketCapApi.get_metadata",
        return_value=example_metadata_api_return(),
    )
    def test_generate_universe(self, mock_get_metadata, clean_test_directory):
        universe_step = UniverseStep(tc.TEST_TIMESTAMP)
        # Overwriting data location properties to use predictable test locations
        universe_step.universe_base_path = tc.TEMP_UNIVERSE_DIRECTORY
        universe_step.listings_base_path = tc.MOCK_LISTINGS_DIRECTORY
        df = universe_step.generate_universe()

        # Little hacky, but there are 150000 ids in the mock list.
        # We make 1 metadata call for every 240 items.
        # We expect to make 63 calls, returning 4 items each time.
        # 4 * 63 = 252. So we expect the df to have 252 objects.
        assert len(df) == 252
        assert (
            len(os.listdir(tc.TEMP_UNIVERSE_DIRECTORY)) == 1
        ), "Test data should have been written out to the data lake location"
