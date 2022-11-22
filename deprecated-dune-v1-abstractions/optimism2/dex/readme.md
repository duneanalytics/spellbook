# DEX Trades

- **dex.trades**: Creates the dex.trades schema, intended to match the table in the ethereum schema. 

Since we do not yet have price feeds for Optimism tokens, these tables pull USD amounts from **prices.approx_prices_from_dex_data**. *Once we have accurate price feeds, this can be migrated.*


#### Currently Includes DEXs & Insert Queries
- **dex.prices_and_trades_inserts**: Top-level cron insert for chainlink prices, dex prices, dex trades, and price backfills.

- **dex.insert_uniswap_v3.sql**: Inserts uniswap v3 data to the dex.trades table.
- **dex.insert_onceinch.sql**: Inserts 1inch (V3, V4) data to the dex.trades table.
- **dex.insert_zeroex.sql**: 0x and Matcha
- **dex.insert_clipper.sql**: Clipper DEX
- **dex.insert_curve.sql**: Curve - Each pool type has different setup (i.e. Stableswap vs Metapools vs Other factory pools)
- **dex.insert_zipswap.sql**: Zipswap calldata optimized DEX
- **dex.insert_kwenta.sql**: Synthetix's Kwenta spot trading. These trades determined are based on ERC20 transfers.
- **dex.insert_rubicon.sql**: Rubicon Order Book DEX. We only add trade logs (filled orders) here in order to align with AMM DEXs.
- **dex.insert_wardenswap.sql**: WardenSwap DEX Aggregator. This has it's own pools as well, but seems to always route through other DEXs.

#### Remaining to be added
Want to contribute to building dex.trades on Optimism? Here are some examples of DEXs and Aggregators that need insert queries built:
- _**DEXs:** Kromatika, etc_
- _**Aggregators:** Slingshot, Firebird, KyberSwap_


[OP DeFi Apps List](https://www.optimism.io/apps/defi)
