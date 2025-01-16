# The issue here is that we have a ticker symbol for a security, not a cryptocurrency

curl --location --request GET 'pro-api.coinmarketcap.com/v2/cryptocurrency/info?symbol=BTC,ETH,SOL,DOGE,TRX,OP,ALGO,APPL,ATOM,XMR,AAVE,LTC,DOT,UNI,SHIB,APE,CORE,DASH,SUSHI,LUNA' \
--header 'X-CMC_PRO_API_KEY: {{INSERT API KEY HERE}}' \
--header 'Accept: */*'