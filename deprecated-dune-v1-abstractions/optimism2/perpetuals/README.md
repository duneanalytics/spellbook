# Perpetuals Trades

- **perpetuals.trades**: Builds a consolidated perpetual swaps table on Optimism. Patterned after the dex.trades table.
 
### Current Projects
- Kwenta/Synthetix
- Perpetual Protocol (v2)
- Pika Protcol (v1 and v2)

### Insert Queries
- **perpetuals.insert_sythetix.sql**: Inserts Kwenta/Synthetix data to the perpetuals.trades table.
- **perpetuals.insert_perpetual.sql**: Inserts Perpetual data to the perpetuals.trades table.
- **perpetuals.insert_pika_v1.sql**: Inserts Pika Protocol v1 data to the perpetuals.trades table.
- **perpetuals.insert_pika_v2.sql**: Inserts Pika Protocol v2 data to the perpetuals.trades table.

### Table Columns
- block_time
  - Time of the transaction
- virtual_asset
  - How the protocol represents the underlying asset
- underlying_asset
  - The real underlying asset that is represented in the swap 
- market
  - The futures market involved in the transaction
- market_address
  - Contract address of the market
- volume_usd
  - The size of the position taken for the swap in USD
  - Already in absolute value and decimal normalized 
    - 18 decimals for Synthetix and Perpetual
    - 8 decimals for Pika
- fee_usd
  - The fees charged to the user for the swap in USD
- margin_usd
  - The amount of collateral/margin used in a trade in USD
- trade
  - Indicates a trade’s direction whether a short, long, or if a position is being closed
- project
  - The protocol/project where the swap took place
- version
  - The version of the protocol/project
- trader
  - The address which made the swap in the protocol
- volume_raw
  - The size of the position in raw form, based on the protocol’s specifications
- tx_hash
  - The hash of the transaction
- tx_from
  - The address that originated the transaction
  - This is based on the optimism.transactions table
- tx_to
  - The address receiving the transaction
  - This is based on the optimism.transactions table
- evt_index
  - Event index number
- trade_id
  - Unique trade id based on project, tx_hash, evt_index


[OP DeFi Apps List](https://www.optimism.io/apps/defi)
