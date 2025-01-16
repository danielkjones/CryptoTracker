# This is paginated. If you need all data will need to go through multiple calls

curl --location --request GET 'pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest?start=1&limit=100&sort=market_cap&cryptocurrency_type=all&tag=all' \
--header 'X-CMC_PRO_API_KEY: {{INSERT API KEY}}' \
--header 'Accept: */*'