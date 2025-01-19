import os
from test.helpers import TestConstants as tc
from test.helpers import delete_directory_contents

import pytest
from pandas import DataFrame

from src.steps.pricing import PricingStep
from src.util.exceptions import InvalidSymbolException

# NOTE: Mock / Temp data locations have been moved to test.helpers.TestConstants
#  to avoid repeats.


@pytest.fixture
def clean_test_directory():
    """Test writes out to Temp Pricing Directory. Need to clean up
    before and after tests.
    """
    delete_directory_contents(tc.TEMP_PRICING_DIRECTORY)
    yield
    delete_directory_contents(tc.TEMP_PRICING_DIRECTORY)


class TestPricingStep:

    def test_generate_pricing_error(self):

        pricing = PricingStep(tc.TEST_TIMESTAMP)
        # Overwriting data location properties to use predictable test locations
        pricing.configuration_file_directory = tc.MOCK_CONFIGURATION_DIRECTORY
        pricing.configuration_file_name = tc.MOCK_BAD_CONFIGURATION_FILE_NAME
        pricing.listings_file_directory = tc.MOCK_LISTINGS_DIRECTORY

        with pytest.raises(
            InvalidSymbolException,
            match="Following Symbols are not valid Coin Symbols: \['APPL']. Please remove from the coins_to_track.csv configuration and re-run process.",
        ):
            pricing.generate_pricing()

    def test_generate_pricing(self, clean_test_directory):
        pricing = PricingStep(tc.TEST_TIMESTAMP)
        # Overwriting data location properties to use predictable test locations
        pricing.configuration_file_directory = tc.MOCK_CONFIGURATION_DIRECTORY
        pricing.listings_file_directory = tc.MOCK_LISTINGS_DIRECTORY
        pricing.pricing_file_directory = tc.TEMP_PRICING_DIRECTORY

        df = pricing.generate_pricing()

        assert len(df) == 19
        assert df["IsTopCurrency"] is not None
        assert df["LoadedWhen"] is not None
        assert (
            len(set(list(df["symbol"]))) == 19
        ), "There should be no duplicate symbols"

        assert (
            len(os.listdir(tc.TEMP_PRICING_DIRECTORY)) == 1
        ), "Test data should have been written out to the data lake location"
