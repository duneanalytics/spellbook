### DEX

- **dex.trades**: Creates the dex.trades schema, intended to match the table in the ethereum schema. 

Since we do not yet have price feeds for Optimism tokens, these tables pull USD amounts from **prices.approx_prices_from_dex_data**. *Once we have accurate price feeds, this can be migrated.*

#### Insert Queries

- **dex.insert_uniswap_v3.sql**: Inserts uniswap v3 data to the dex.trades table.

_Other DEXs on Optimism to be added: 1Inch, Rubicon, Kwenta (Synthetix), [add to list]_
