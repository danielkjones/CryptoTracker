# CryptoTracker

## TODO

Feature 
- Need to have that function that reads the output, prints the output
- Fix timestamps in the CSVs
- Basic logging
- Error handling if any of the datasets do not exist


Testings
- Confirm that the CSV outputs all make sense
- Run end-to-end test on the code


Quality
- Ensure proper commenting
- Do we need to do anything with the folder naming so it makes more sense?
- README


## DONE 
Feature
- Need workflow that combines all these processes into one
- Adjust the universe to read dataset instead of file input
- Retries on API calls
- Nice to have: if there is a failure, can re-run with a timestamp (think DAG re-run)
- Confirm all the test cases actually do the thing that is expected

Quality
- Switch to using constants 
- Shared helpers and fixtures for testing
- Shared setup / teardown (can create and delete the temp directories ahead of time)
- Move the mock responses from the API to their own mock location in test

Testing
- Test that the Metadata API is not going to fail with a large number of symbols 


# Setup

Create a `.env` file in the root directory with the following values

```
COIN_MARKET_CAP_ACCESS_KEY = {your access key}
COIN_MARKET_CAP_HOST = {"https://pro-api.coinmarketcap.com/" | "https://sandbox-api.coinmarketcap.com"}
```
