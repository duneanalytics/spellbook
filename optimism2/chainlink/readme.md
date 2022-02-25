# Chainlink Price Oracle Updates - Optimism
The following queries create a prices table based on Optimism Chainlink Oracle updates. These may be used as the base "source" of truth to populate prices for DEX trades and other relevant tables.

To add a new price feed, add a row to the `oracle_addresses.sql` table with the following information:
- **feed_name**: Sourced from [Chainlink Docs](https://docs.chain.link/docs/optimism-price-feeds/)
- **decimals**: Sourced from [Chainlink Docs](https://docs.chain.link/docs/optimism-price-feeds/) (Note: This refers to the price feed deciamls, not the underlying token's decimals)
- **proxy**: Sourced from [Chainlink Docs](https://docs.chain.link/docs/optimism-price-feeds/)
- **address**: Sourced from opening the proxy address on Etherscan ([example](https://optimistic.etherscan.io/address/0x338ed6787f463394D24813b297401B9F05a8C9d1#readContract)), clicking 'Contract' -> 'Read Contract' and getting the address from the 'aggregator' field
