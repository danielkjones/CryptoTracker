import os
from os.path import dirname, exists, join
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

    def test_generate_bitcoin_comparison(self, clean_test_directory):
        step = BitcoinComparisonStep(tc.TEST_TIMESTAMP)
        # Overwriting data location properties to use predictable test locations
        step.pricing_file_directory = tc.MOCK_PRICING_DIRECTORY
        step.listings_file_directory = tc.MOCK_LISTINGS_DIRECTORY
        step.bitcoin_comparison_directory = tc.TEMP_BITCOIN_COMPARISONS_DIRECTORY
        df = step.generate_bitcoin_comparison()

        assert df["ID"] is not None
        assert df["Symbol"] is not None
        assert df["Name"] is not None
        assert df["BitcoinVsCurrency24hPercentChangeDiff"] is not None
        assert df["PercentChange24h"] is not None
        assert df["BitcoinPercentChange24h"] is not None
        assert df["LoadedWhen"] is not None

        assert exists(
            tc.TEMP_BITCOIN_COMPARISONS_DIRECTORY
        ), "File should have been written out to data lake location"
