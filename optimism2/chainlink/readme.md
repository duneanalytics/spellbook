# Chainlink Price Oracle Updates - Optimism
The following queries create a prices table based on Optimism Chainlink Oracle updates. These may be used as the base "source" of truth to populate prices for DEX trades and other relevant tables.

The actual cron job insert is housed in dex.prices_and_trades_inserts .

To add a new price feed, add a row to the `oracle_addresses.sql` table with the following information:
- **feed_name**: Sourced from [Chainlink Docs](https://docs.chain.link/docs/optimism-price-feeds/)
- **decimals**: Sourced from [Chainlink Docs](https://docs.chain.link/docs/optimism-price-feeds/) (Note: This refers to the price feed deciamls, not the underlying token's decimals)
- **proxy**: Sourced from [Chainlink Docs](https://docs.chain.link/docs/optimism-price-feeds/)
- **address**: Sourced from opening the proxy address on Etherscan ([example](https://optimistic.etherscan.io/address/0x338ed6787f463394D24813b297401B9F05a8C9d1#readContract)), clicking 'Contract' -> 'Read Contract' and getting the address from the 'aggregator' field

To map this price feed to an ERC20 token, add a row to the `oracle_token_mapping.sql` table with the following information:
- **underlying_token_address**: Manually match the price feed to the ERC20 token that it represents. *Note: Data quality is paramount here. Make sure you select the [Optimism](https://optimistic.etherscan.io/) token address, which can be different than L1. Also check for token quality (i.e. # of holders) as some tokens may not yet be bridged to Optimism, or have non-canonical clones.*
- **extra_decimals**: This is rare, but some tokens take a price feed, then adjust the decimals in order to create the price for their token *(i.e. TCAP uses the Total Crypto Market Cap feed, but divides the price by 10^10 [10 decimals])*

*Contact @MSilb7 on Twitter, Dune Discord, or in the analytics channel in Optimism Discord with any questions.*
