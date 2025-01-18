import os
from os.path import dirname, join
from test.helpers import TestConstants as tc
from test.helpers import delete_directory_contents

import pytest

from src.steps.bitcoin_comparison import BitcoinComparisonStep

# NOTE: Test Constants and helpers live in test.helpers to
#  avoid repeat work


@pytest.fixture
def clean_test_directory():
    """Test writes out to Temp Bitcoin Comparison Directory. Need to clean up
    before and after tests.
    """
    delete_directory_contents(tc.TEMP_BITCOIN_COMPARISONS_DIRECTORY)
    yield
    delete_directory_contents(tc.TEMP_BITCOIN_COMPARISONS_DIRECTORY)


class TestBitcoinComparison:

    def test_unpack_percent_change_24h(self):
        example_quote_string = "{'USD': {'price': 99203.96568075211, 'volume_24h': 58240590207.8066, 'volume_change_24h': 15.2907, 'percent_change_1h': 0.15811113, 'percent_change_24h': 2.76610597, 'percent_change_7d': 5.98901655, 'percent_change_30d': -7.17412121, 'percent_change_60d': 9.20697253, 'percent_change_90d': 46.59964611, 'market_cap': 1965320141316.709, 'market_cap_dominance': 56.0444, 'fully_diluted_market_cap': 2083283279295.79, 'tvl': None, 'last_updated': '2025-01-16T12:31:00.000Z'}}"
        step = BitcoinComparisonStep(tc.TEST_TIMESTAMP)
        percent_change = step.unpack_percent_change_24h(example_quote_string)
        assert percent_change == 2.76610597

    def test_generate_bitcoin_comparison(self, clean_test_directory):
        step = BitcoinComparisonStep(tc.TEST_TIMESTAMP)
        # Overwriting data location properties to use predictable test locations
        step.pricing_file_directory = tc.MOCK_PRICING_DIRECTORY
        step.listings_file_directory = tc.MOCK_LISTINGS_DIRECTORY
        step.bitcoin_comparison_directory = tc.TEMP_BITCOIN_COMPARISONS_DIRECTORY
        df = step.generate_bitcoin_comparison()

        assert df["percent_change_24h"] is not None
        assert df["24h_against_bitcoin"] is not None
        assert df["bitcoin_percent_change_24h"] is not None
        assert df["LoadedWhen"] is not None

        assert (
            len(os.listdir(tc.TEMP_BITCOIN_COMPARISONS_DIRECTORY)) == 1
        ), "File should have been written out to data lake location"
