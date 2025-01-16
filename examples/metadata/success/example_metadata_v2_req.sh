# All valid cryptocurrency values

curl --location --request GET 'pro-api.coinmarketcap.com/v2/cryptocurrency/info?symbol=BTC,ETH,SOL,DOGE' \
--header 'X-CMC_PRO_API_KEY: {{INSERT API KEY HERE}}' \
--header 'Accept: */*'