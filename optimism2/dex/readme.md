# DEX Trades

- **dex.trades**: Creates the dex.trades schema, intended to match the table in the ethereum schema. 

Since we do not yet have price feeds for Optimism tokens, these tables pull USD amounts from **prices.approx_prices_from_dex_data**. *Once we have accurate price feeds, this can be migrated.*

**Currently Included:** Uniswap V3, 1inch, 0x (Matcha)

#### Insert Queries
- **dex.prices_and_trades_inserts**: Top-level cron insert for chainlink prices, dex prices, dex trades, and price backfills.

- **dex.insert_uniswap_v3.sql**: Inserts uniswap v3 data to the dex.trades table.
- **dex.insert_onceinch.sql**: Inserts 1inch (V3, V4) data to the dex.trades table.
- **dex.insert_zeroex.sql**

#### Remaining to be added
Want to contribute to building dex.trades on Optimism? Here are some examples of DEXs and Aggregators that need insert queries built:
- _**DEXs:** Rubicon, Kwenta (Synthetix), ZipSwap, Clipper, etc_
- _**Aggregators:** Slingshot_

_Other Cases (how to handle this - TBD)_

To be figured out if these should belong in dex.trades or their own abstraction layers
- _**Perpetuals:** Perpetual Protocol, Pika Protocol, Kewnta shorts_
- _**Options:** Lyra, Thales_

[OP DeFi Apps List](https://www.optimism.io/apps/defi)
