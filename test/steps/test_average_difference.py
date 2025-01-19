import os
from os.path import dirname, join
from test.helpers import TestConstants as tc
from test.helpers import delete_directory_contents

import pytest

from src.steps.average_difference import AverageDifferenceStep

# NOTE: Test Constants and helpers live in test.helpers to
#  avoid repeat work


@pytest.fixture
def clean_test_directory():
    """Test writes out to Temp Avg Bitcoin Diff Directory. Need to clean up
    before and after tests.
    """
    delete_directory_contents(tc.TEMP_AVG_BITCOIN_DIFF_DIRECTORY)
    yield
    delete_directory_contents(tc.TEMP_AVG_BITCOIN_DIFF_DIRECTORY)


class TestAverageDifference:

    def test_generate_average_difference(self, clean_test_directory):
        step = AverageDifferenceStep(tc.TEST_TIMESTAMP)
        step.bitcoin_comparison_directory = tc.MOCK_BITCOIN_COMPARISONS_DIRECTORY
        step.avg_bitcoin_diff_directory = tc.TEMP_AVG_BITCOIN_DIFF_DIRECTORY

        df = step.generate_average_difference()

        assert len(df) == 19
        assert df["AvgBitcoinVsCurrency24hPercentChangeDiff"] is not None
        assert (
            len(list(set(df["Symbol"]))) == 19
        ), "There should be no repeats with the aggregation"
        assert (
            len(os.listdir(tc.TEMP_AVG_BITCOIN_DIFF_DIRECTORY)) == 1
        ), "Test data should have been written out to the data lake location"
