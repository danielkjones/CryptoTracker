import json
import os
from os.path import dirname, join
from typing import Dict, List


class TestConstants:
    """Collection of test constants that are used across multiple test cases
    'MOCK_' prefix means that this is consistent across all test cases, and stored
        in the repo consistently.
    'TEMP_' prefix means that this is related to the output of a specific test,
        and should be deleted after the tests are completed.
    """

    # All mock / test data should operate off of this timestamp
    TEST_TIMESTAMP = "20250116000000"

    # Mock API response locations, to be read in for tests cases
    MOCK_LISTINGS_API_RESPONSE_LOCATION = join(
        dirname(__file__), "mock_api_response/example_listings_latest_res.json"
    )
    MOCK_METADATA_API_RESPONSE_LOCATION = join(
        dirname(__file__), "mock_api_response/example_metadata_v1_res.json"
    )

    # Mock data locations, to be read in for test cases
    MOCK_BITCOIN_COMPARISONS_DIRECTORY = join(
        dirname(__file__), "mock_data_lake/bitcoin_comparison"
    )
    MOCK_CONFIGURATION_DIRECTORY = join(
        dirname(__file__), "mock_data_lake/configuration"
    )
    MOCK_BAD_CONFIGURATION_FILE_NAME = "bad_coins_to_track.csv"
    MOCK_LISTINGS_DIRECTORY = join(dirname(__file__), "mock_data_lake/listings")
    MOCK_PRICING_DIRECTORY = join(dirname(__file__), "mock_data_lake/pricing")
    MOCK_UNIVERSE_DIRECTORY = join(dirname(__file__), "mock_data_lake/universe")

    # Temp data locations, to be written to from test cases and cleaned up
    TEMP_AVG_BITCOIN_DIFF_DIRECTORY = join(
        dirname(__file__), "temp_data_lake/avg_bitcoin_diff"
    )
    TEMP_BITCOIN_COMPARISONS_DIRECTORY = join(
        dirname(__file__), "temp_data_lake/bitcoin_comparison"
    )
    TEMP_CONFIGURATION_DIRECTORY = join(
        dirname(__file__), "temp_data_lake/configuration"
    )
    TEMP_LISTINGS_DIRECTORY = join(dirname(__file__), "temp_data_lake/listings")
    TEMP_PRICING_DIRECTORY = join(dirname(__file__), "temp_data_lake/pricing")
    TEMP_UNIVERSE_DIRECTORY = join(dirname(__file__), "temp_data_lake/universe")


# GENERAL HELPERS


def delete_directory_contents(directory: str) -> None:
    """Delete the contents of a given directory

    Args:
        directory (str): Directory to delete files from
    """
    for filename in os.listdir(directory):
        file_path = os.path.join(directory, filename)
        if os.path.isfile(file_path):
            os.remove(file_path)


# API RESPONSE HELPERS


def example_listings_api_return() -> Dict:
    """Sample response of what would be returned by the listings
    API. To be used in test cases.

    Returns:
        Dict: Dict of what would be returned by the API directly
    """
    with open(TestConstants.MOCK_LISTINGS_API_RESPONSE_LOCATION, "r") as file:
        jsn = json.load(file)
        return jsn


def example_metadata_api_return() -> List[Dict]:
    """Retrieve the example return from the listings call. Is slightly
    modified from the original dict since we modify in the API class.

    Returns:
        List[Dict]: List of the metadata on coins that would be returned by the
            metadata API
    """
    # Traversing up directories using dirname()
    with open(TestConstants.MOCK_METADATA_API_RESPONSE_LOCATION, "r") as file:
        jsn = json.load(file)
        return list(jsn["data"].values())
