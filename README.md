# CryptoTracker

Program that gathers market data on all active cryptocurrencies and generates views for tracking
pricing and daily changes against Bitcoin.

## Program Setup

### Installation

1. You must have Python 3.12 installed on your machine
2. You must have `pipenv` installed on your machine
3. From the root directory run `pipenv install` to generate a virtual environment with all required dependencies


### CoinMarketCap API (CMC API) Setup
You need an account and access key with [CoinMarketCap API](https://coinmarketcap.com/api/) to run this program.

Create a free account and access key from the [CoinMarketCap API Signup Page](https://coinmarketcap.com/api/pricing/).


### Environment File Setup
Create a `.env` file in the root directory of this project with the following values

```
COIN_MARKET_CAP_ACCESS_KEY = {your access key for CoinMarketCap API}
COIN_MARKET_CAP_HOST = "https://pro-api.coinmarketcap.com/"
```

## Running the Program

### Running Program

Once you have installation, access key, and `.env` file setup you can run the program with the following command:

```
pipenv run python3 crypto_tracker_workflow.py
```

This will run a fresh execution that recreates all datasets for the execution.

If you have a failure in an execution and you do not want to recreate all the files that were generated you can 
run the following command with a `--timestamp=` command line argument:

```
pipenv run python3 crypto_tracker_workflow.py --timestamp={execution_timestamp}
```

Where execution timestamp is a UTC timestamp in YYYYMMDDHHMMSS format. This is shown in the output logs 
as well as appended to any .csv files that were created from the previous execution.

### Modifying Coins to Track Configuration

Place crypto symbols that you would like to track in the `data_lake/configuration/coins_to_track.csv`
file. Make sure to have only one symbol per line. 

Make sure Line 1 is always `Symbol`, but feel free to add or remove other symbols.

## Dataset Information

This program produces the following datasets (with the exception of `Coins to Track` which is user input):

| Dataset                   | Data Directory                | File Format                           | Classification       | Description                                                                                              |
|--------------------------- |------------------------------|---------------------------------------|----------------------|----------------------------------------------------------------------------------------------------------|
| Coins to Track            | data_lake/configuration/      | coins_to_track.csv                    | Static Configuration | User input collection of crypto symbols to track.                                                       |
| Listings                  | data_lake/listings/           | crypto_listings_YYYYMMDDHHMMSS.csv    | Bronze               | Raw listing data of active cryptocurrencies from CMC API.                                               |
| Universe                  | data_lake/universe/           | crypto_universe_YYYYMMDDHHMMSS.csv    | Bronze               | Raw cryptocurrency metadata for all active currencies from CMC API.                                     |
| Pricing                   | data_lake/pricing/            | coins_pricing_YYYYMMDDHHMMSS.csv      | Silver               | Normalized pricing data on all cryptocurrencies provided in `Coins to Track`.                           |
| Bitcoin Comparison        | data_lake/bitcoin_comparison/ | bitcoin_comparison_YYYYMMDDHHMMSS.csv | Gold                 | Comparison of 24-hour percentage change of cryptocurrency symbols in `Coins to Track` against Bitcoin's 24-hour percentage change. |
| Average Bitcoin Difference| data_lake/avg_bitcoin_diff    | avg_bitcoin_diff_YYYYMMDDHHMMSS.csv   | Gold                 | The average difference of 24-hour percentage change of cryptocurrency symbols in `Coins to Track` against Bitcoin's 24-hour percentage change for each day the process is run. |


**NOTE**: Each dataset has a UTC timestamp in the format `YYYYMMDDHHMMSS` appended to the filename. Files with the same 
timestamp belong to the same execution. If your process fails part way through, you can re-run the program with the flag `--timestamp={UTC timestamp}`
to pick up the process where it left off. 

*This may be especially useful if you are modifying the `Coins to Track` dataset and you run into errors*.  


## Devs Only

### Testing
Run following to run unit tests from command line 
```
pipenv run pytest
```
