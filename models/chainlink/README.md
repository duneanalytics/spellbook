# Price Feeds

## Optimism 

To add a new price feed, add a row to the `chainlink_optimism_price_feeds_oracle_addresses.sql` table with the following information:
- **feed_name**: Sourced from [Chainlink Docs](https://docs.chain.link/docs/optimism-price-feeds/)
- **decimals**: Sourced from [Chainlink Docs](https://docs.chain.link/docs/optimism-price-feeds/) (Note: This refers to the price feed decimals, not the underlying token's decimals)
- **proxy_address**: Sourced from [Chainlink Docs](https://docs.chain.link/docs/optimism-price-feeds/)
- **aggregator_address**: Sourced from opening the proxy address on Etherscan ([example](https://optimistic.etherscan.io/address/0x338ed6787f463394D24813b297401B9F05a8C9d1#readContract)), clicking 'Contract' -> 'Read Contract' and getting the address from the 'aggregator' field

*Open Research Area: Is there a way for us to deterministically build the oracle -> address -> feed name -> token links purely by reading on-chain events vs manually entering data from Chainlink docs?*
