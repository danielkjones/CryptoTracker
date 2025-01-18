from os import getenv
from os.path import dirname, join

TIMESTAMP_FORMAT = "%Y%m%d%H%M%S"

# DATASET LOCATIONS
#
# In a more complicated system this could be moved to a configuration file
# that may or may not be shared across different systems. Keeping in a constants
# file for now, as this will change between computers.

AVG_BITCOIN_DIFF_DATA_LOCATION = join(
    dirname(dirname(dirname(__file__))), "data_lake/avg_bitcoin_diff"
)
AVG_BITCOIN_DIFF_CSV_FORMAT = "avg_bitcoin_diff_{}.csv"


BITCOIN_COMPARISON_DATA_LOCATION = join(
    dirname(dirname(dirname(__file__))), "data_lake/bitcoin_comparison"
)
BITCOIN_COMPARISON_CSV_FORMAT = "bitcoin_comparison_{}.csv"

COINS_TO_TRACK_DATA_LOCATION = join(
    dirname(dirname(dirname(__file__))), "data_lake/configuration"
)
COINS_TO_TRACK_CSV_NAME = "coins_to_track.csv"

LISTINGS_DATA_LOCATION = join(dirname(dirname(dirname(__file__))), "data_lake/listings")
LISTINGS_CSV_FORMAT = "crypto_listings_{}.csv"

PRICING_DATA_LOCATION = join(dirname(dirname(dirname(__file__))), "data_lake/pricing")
PRICING_CSV_FORMAT = "coins_pricing_{}.csv"

UNIVERSE_DATA_LOCATION = join(dirname(dirname(dirname(__file__))), "data_lake/universe")
UNIVERSE_CSV_FORMAT = "crypto_universe_{}.csv"

# API INFORMATION
COIN_MARKET_CAP_HOST = getenv("COIN_MARKET_CAP_HOST")
COIN_MARKET_CAP_ACCESS_KEY = getenv("COIN_MARKET_CAP_ACCESS_KEY")
