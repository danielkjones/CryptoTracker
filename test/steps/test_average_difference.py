import os
from os.path import dirname, join

import pytest

from src.steps.average_difference import AverageDifferenceStep

TEST_TIMESTAMP = "20250116000000"
TEST_BITCOIN_COMPARISON_DIRECTORY = join(
    dirname(dirname(__file__)), "mock_data_lake/bitcoin_comparison"
)
TEST_AVG_BITCOIN_DIFF_DIRECTORY = join(
    dirname(dirname(__file__)), "temp_data_lake/avg_bitcoin_diff"
)


def delete_directory_contents(directory: str) -> None:
    """Delete the contents of a given directory

    Args:
        directory (str): Directory to delete files from
    """
    for filename in os.listdir(directory):
        file_path = os.path.join(directory, filename)
        if os.path.isfile(file_path):
            os.remove(file_path)


@pytest.fixture
def clean_test_directory():
    delete_directory_contents(TEST_AVG_BITCOIN_DIFF_DIRECTORY)
    yield
    delete_directory_contents(TEST_AVG_BITCOIN_DIFF_DIRECTORY)


class TestAverageDifference:

    def test_generate_average_difference(self, clean_test_directory):
        step = AverageDifferenceStep(TEST_TIMESTAMP)
        step.bitcoin_comparison_directory = TEST_BITCOIN_COMPARISON_DIRECTORY
        step.avg_bitcoin_diff_directory = TEST_AVG_BITCOIN_DIFF_DIRECTORY

        df = step.generate_average_difference()

        assert len(df) == 19
        assert df["Avg_24h_Bitcoin_Diff"] is not None
        assert (
            len(list(set(df["symbol"]))) == 19
        ), "There should be no repeats with the aggregation"
        assert (
            len(os.listdir(TEST_AVG_BITCOIN_DIFF_DIRECTORY)) == 1
        ), "Test data should have been written out to the data lake location"
