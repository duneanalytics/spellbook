### DEX

- **dex.trades**: Creates the dex.trades schema, intended to match the table in the ethereum schema. 

Since we do not yet have price feeds for Optimism tokens, these tables pull USD amounts from **prices.approx_prices_from_dex_data**. *Once we have accurate price feeds, this can be migrated.*

#### Insert Queries

- **dex.insert_uniswap_v3.sql**: Inserts uniswap v3 data to the dex.trades table.

_Other DEXs on Optimism to be added: Rubicon, Kwenta (Synthetix), ZipSwap, Juggler, (add to list)_
- _Aggregators: Matcha, 1Inch_
- _Perpetuals (figure out how to handle this): Perpetual Protocol, Pika Protocol, Kewnta shorts_

[OP DeFi Apps List](https://www.optimism.io/apps/defi)
