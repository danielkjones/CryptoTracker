# CryptoTracker

## TODO

Feature 
- Fix timestamps in the CSVs
- Need to have a step that will read the output and print / log for the end user
- Basic logging
- Trim / Rename data that is in the pricing data frame


Testings
- Confirm that the CSV outputs all make sense
- Run end-to-end test on the code


Quality
- Ensure proper commenting
- README


## DONE 

Feature
- Potentially unpack the Pricing data further. It's not usable for an analyst
- Error handling if any of the datasets do not exist



# Setup

Create a `.env` file in the root directory with the following values

```
COIN_MARKET_CAP_ACCESS_KEY = {your access key}
COIN_MARKET_CAP_HOST = {"https://pro-api.coinmarketcap.com/"}
```
