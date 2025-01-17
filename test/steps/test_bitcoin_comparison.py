import os
from os.path import dirname, join

import pytest

from src.steps.bitcoin_comparison import BitcoinComparisonStep

TEST_TIMESTAMP = "20250116000000"
TEST_PRICING_DIRECTORY = join(dirname(dirname(__file__)), "mock_data_lake/pricing")
TEST_LISTINGS_DIRECTORY = join(dirname(dirname(__file__)), "mock_data_lake/listings")
TEST_BITCOIN_COMPARISON_DIRECTORY = join(
    dirname(dirname(__file__)), "temp_data_lake/bitcoin_comparison"
)


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
    delete_directory_contents(TEST_BITCOIN_COMPARISON_DIRECTORY)
    yield
    delete_directory_contents(TEST_BITCOIN_COMPARISON_DIRECTORY)


class TestBitcoinComparison:

    def test_unpack_percent_change_24h(self):
        example_quote_string = "{'USD': {'price': 99203.96568075211, 'volume_24h': 58240590207.8066, 'volume_change_24h': 15.2907, 'percent_change_1h': 0.15811113, 'percent_change_24h': 2.76610597, 'percent_change_7d': 5.98901655, 'percent_change_30d': -7.17412121, 'percent_change_60d': 9.20697253, 'percent_change_90d': 46.59964611, 'market_cap': 1965320141316.709, 'market_cap_dominance': 56.0444, 'fully_diluted_market_cap': 2083283279295.79, 'tvl': None, 'last_updated': '2025-01-16T12:31:00.000Z'}}"
        step = BitcoinComparisonStep(TEST_TIMESTAMP)
        percent_change = step.unpack_percent_change_24h(example_quote_string)
        assert percent_change == 2.76610597

    def test_generate_bitcoin_comparison(self, clean_test_directory):
        step = BitcoinComparisonStep(TEST_TIMESTAMP)
        step.pricing_file_directory = TEST_PRICING_DIRECTORY
        step.listings_file_directory = TEST_LISTINGS_DIRECTORY
        step.bitcoin_comparison_directory = TEST_BITCOIN_COMPARISON_DIRECTORY
        df = step.generate_bitcoin_comparison()

        assert df["percent_change_24h"] is not None
        assert df["24h_against_bitcoin"] is not None
        assert df["bitcoin_percent_change_24h"] is not None
        assert df["LoadedWhen"] is not None

        assert (
            len(os.listdir(TEST_BITCOIN_COMPARISON_DIRECTORY)) == 1
        ), "File should have been written out to data lake location"
