Dune's `erc20.token_balances` table holds snapshots of token balances for Ethereum wallet addresses. At the time of writing, `erc20.token_balances` contains 605 Million rows, and so we need abstractions on top of this table to make its data more easily accessible.

One abstraction that the token balances data enables is to view the liquidity provided to DEXes. This is what `dex.liquidity` aims to do. Many DEXes such as Uniswap v2 have a distinct address for a pool and those pool addresses contain the token assets directly. In such cases, querying the `token_balances` table using the pool and tokens addresses gives us the liquidity over time.

Not all DEXes function this way, e.g. the `Vault` is a single contract that holds the assets added by all Balances v2 pools. Here we need to write custom logic to get the token balances.

:warning: `dex.liquidity` is available in Dune's Ethereum database. You can use and query this table but please proceed with caution. The entries for some pools shows erroneous negative token balances (~ 0.7%). These come from the underlying erc20.token_balances table and have been reported to the upstream engineers. :warning:
