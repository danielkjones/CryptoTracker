import os
from os.path import dirname, join

import pytest
from pandas import DataFrame

from src.steps.pricing import PricingStep
from src.util.exceptions import InvalidSymbolException

TEST_CONFIGURATION_DIRECTORY = join(
    dirname(dirname(__file__)), "mock_data_lake/configuration"
)
TEST_BAD_CONFIGURATION_FILE = "bad_coins_to_track.csv"
TEST_LISTINGS_DIRECTORY = join(dirname(dirname(__file__)), "mock_data_lake/listings")
TEST_TIMESTAMP = "20250116000000"

TEST_PRICING_DIRECTORY = join(dirname(dirname(__file__)), "temp_data_lake/pricing")


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
    delete_directory_contents(TEST_PRICING_DIRECTORY)
    yield
    delete_directory_contents(TEST_PRICING_DIRECTORY)


class TestPricingStep:

    def test_get_coins_to_track(self):
        pricing = PricingStep(TEST_TIMESTAMP)
        pricing.configuration_file_directory = TEST_CONFIGURATION_DIRECTORY

        df = pricing.read_coins_to_track()
        assert isinstance(df, DataFrame)

    def test_get_listings(self):
        pricing = PricingStep(TEST_TIMESTAMP)
        pricing.listings_file_directory = TEST_LISTINGS_DIRECTORY
        df = pricing.read_listings()
        assert isinstance(df, DataFrame)

    def test_generate_pricing_error(self):
        pricing = PricingStep(TEST_TIMESTAMP)
        pricing.configuration_file_directory = TEST_CONFIGURATION_DIRECTORY
        pricing.configuration_file_name = TEST_BAD_CONFIGURATION_FILE
        pricing.listings_file_directory = TEST_LISTINGS_DIRECTORY

        with pytest.raises(
            InvalidSymbolException,
            match="Following Symbols are not valid Coin Symbols: \['APPL']. Please remove from the coins_to_track.csv configuration and re-run process.",
        ):
            pricing.generate_pricing()

    def test_generate_pricing(self, clean_test_directory):
        pricing = PricingStep(TEST_TIMESTAMP)
        pricing.configuration_file_directory = TEST_CONFIGURATION_DIRECTORY
        pricing.listings_file_directory = TEST_LISTINGS_DIRECTORY
        pricing.pricing_file_directory = TEST_PRICING_DIRECTORY

        df = pricing.generate_pricing()

        assert len(df) == 19
        assert df["IsTopCurrency"] is not None
        assert df["LoadedWhen"] is not None
        assert (
            len(set(list(df["symbol"]))) == 19
        ), "There should be no duplicate symbols"

        assert (
            len(os.listdir(TEST_PRICING_DIRECTORY)) == 1
        ), "Test data should have been written out to the data lake location"
