import argparse
import logging
import sys
from datetime import datetime, timezone

import pandas as pd

from src.steps import (
    AverageDifferenceStep,
    BitcoinComparisonStep,
    ListingsStep,
    PricingStep,
    UniverseStep,
)
from src.util.config import TIMESTAMP_FORMAT

# Creating Logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Setting STD Out for logging
stdout_handler = logging.StreamHandler(sys.stdout)
stdout_handler.setLevel(logging.INFO)

# Define simple logging format
formatter = logging.Formatter("%(asctime)s : %(message)s")
stdout_handler.setFormatter(formatter)

# Add the handler to the logger
logger.addHandler(stdout_handler)


def main() -> None:
    """Configure timestamp for the execution and call the main driver.

    The timestamp is either generated from current time in UTC, or from end user input.
    """
    # Optionally can add '--timestamp' arg
    parser = argparse.ArgumentParser(description="Processing any optional flags")
    parser.add_argument(
        "--timestamp",
        help="Optional flag for setting timestamp in YYYYMMDDHHMMSS format for execution. Helpful for re-running partially complete workflows.",
    )
    args = parser.parse_args()

    if args.timestamp:
        logger.info(
            "Timestamp provided. Will attempt to use cached values when executing."
        )
        analysis_timestamp = args.timestamp
    else:
        # Using a singular UTC datetime stamp in YYYYMMDDHHMMSS format.
        # Used so we can track all files generated through a singular execution.
        analysis_timestamp = datetime.now(timezone.utc).strftime(TIMESTAMP_FORMAT)

    run_workflow(analysis_timestamp)


def run_workflow(timestamp: str):
    """Main Driver for running the data workflow

    Args:
        timestamp (str): Timestamp in YYYYMMDDHHMMSS format
    """
    logger.info(f"Starting crypto workflow with following datetime stamp: {timestamp}")

    ######## BRONZE TIER DATASETS ########

    # 1. Generate list of all active Crypto Currencies and save output dataset
    logger.info("Generating list of all active crypto currency listings")
    ListingsStep(timestamp).generate_listings()

    # 2. Use Crypto Currency list to generate universe of metadata
    logger.info(
        "Generating universe of coin metadata for all active crypto currency listings"
    )
    UniverseStep(timestamp).generate_universe()

    ######## SILVER TIER DATASET ########

    # 3. Generate pricing dataset for the coins specified in the coins_to_track.csv input
    logger.info(
        "Generating pricing dataset for coins specified in 'data_lake/configuration/coins_to_track.csv'"
    )
    PricingStep(timestamp).generate_pricing()

    ######## GOLD TIER DATASETS ########
    # 4. Generate bitcoin comparison dataset using pricing dataset and bitcoin values in
    #    coins list
    logger.info(
        "Generating dataset comparing price changes between specified coins and Bitcoin"
    )
    BitcoinComparisonStep(timestamp).generate_bitcoin_comparison()

    # 5. Generate file with average price change difference for each coin across all executions
    logger.info(
        "Generating dataset with average difference in 24h percent change vs Bitcoin across all executions"
    )
    df: pd.DataFrame = AverageDifferenceStep(timestamp).generate_average_difference()


if __name__ == "__main__":
    main()
