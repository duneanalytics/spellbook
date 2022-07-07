# Optimism ERC20 Schema

- `erc20.tokens`: Mapping for token addresses to symbols and decimals
  -  open q: can we pull this from Etherscan or contracts?

- `erc20.daily_token_balances`: Computes the daily token balance for ERC20s and ETH (using the bridge token address)
  - NOTE: This does not yet include token balances before Optimism v4 on July 6th. Addresses who were active then may show inaccurate or negative balaces.
