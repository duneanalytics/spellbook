# Safe Project Documentation

## Namespace Structure

This project follows a two-layer namespace structure to separate raw blockchain data from transformed models:

### 1. Raw Data Layer (`gnosis_safe_*`)
- Location: `sources/safe/*/safe_*_sources.yml`
- Namespace pattern: `gnosis_safe_<network>` (e.g., `gnosis_safe_arbitrum`)
- Purpose: References to raw blockchain event data from Safe contracts that Dune has already indexed
- Example: `gnosis_safe_arbitrum.SafeProxyFactory_v1_3_0_evt_ProxyCreation`

### 2. Model Layer (`safe_*`)
- Location: Models created in `models/_project/safe/` and configured in `dbt_project.yml`
- Namespace pattern: `safe_<network>` (e.g., `safe_arbitrum`)
- Purpose: Transformed and aggregated models that use the raw data to create analytical views
- Example: `safe_arbitrum.safe_arbitrum_transactions`

### How They Connect

In model files, we reference raw data using the `source()` function:

```sql
-- In your model file (e.g., safe_arbitrum_singletons.sql)
FROM {{ source('gnosis_safe_arbitrum', 'SafeProxyFactory_v1_3_0_evt_ProxyCreation') }}
-- This pulls from gnosis_safe_arbitrum namespace (raw data)

-- But the model itself gets created in safe_arbitrum schema
-- as defined in dbt_project.yml (transformed data)
```

This separation ensures:
- Raw blockchain data remains untouched in `gnosis_safe_*` namespaces
- Transformed analytical models are organized in `safe_*` schemas
- Clear distinction between source data and business logic

## Network Configuration

Networks are configured in `macros/project/safe/safe_network_config.sql`. Each network configuration includes:
- `start_date`: When Safe was deployed on that network
- `native_token`: The native token of the network (ETH, BNB, MATIC, etc.)
- `singleton_type`: Type of singleton implementation (legacy, modern, legacy_ethereum)
- `singleton_sources`: List of ProxyFactory event tables to use

## Supported Networks

Currently supported networks:
- Arbitrum
- Avalanche C-Chain
- Base
- Berachain
- Blast
- BNB Chain
- Celo
- Ethereum
- Fantom
- Gnosis
- Linea
- Mantle
- Optimism
- Polygon
- Ronin
- Scroll
- Unichain
- Worldchain
- zkEVM
- zkSync

## Table Naming Conventions

### Source Tables (Raw Data)
All networks should follow this exact naming convention for consistency:
- `SafeProxyFactory_v1_3_0_evt_ProxyCreation` (for v1.3.0)
- `SafeProxyFactory_v1_4_1_evt_ProxyCreation` (for v1.4.1)
- `ProxyFactory_v1_1_1_call_createProxy` (for legacy v1.1.1)

### Model Tables (Transformed Data)
Models follow the pattern `safe_<network>_<type>`:
- `safe_<network>_safes` - All safes deployed on the network
- `safe_<network>_transactions` - All Safe transactions
- `safe_<network>_singletons` - Singleton implementations
- `safe_<network>_<token>_transfers` - Native token transfers (e.g., `safe_ethereum_eth_transfers`)

### Aggregation Models
Cross-chain aggregation models in the root safe folder:
- `safe_safes_all` - All safes across all networks
- `safe_transactions_all` - All transactions across all networks
- `safe_native_transfers_all` - All native transfers across networks that support them

## Adding a New Network

To add support for a new network:

1. Add network configuration to `macros/project/safe/safe_network_config.sql`
2. Create source definitions in `sources/safe/<network>/safe_<network>_sources.yml`
3. Create model files in `models/_project/safe/<network>/`
4. Add schema configuration to `dbt_project.yml`
5. Update aggregation models to include the new network
6. Add tests in `tests/_project/safe/<network>/`

## Column Structure

All source tables should follow the standard column structure (based on Arbitrum reference):

```yaml
columns:
  - contract_address
  - evt_tx_hash
  - evt_tx_from
  - evt_tx_to
  - evt_tx_index
  - evt_index
  - evt_block_time
  - evt_block_number
  - evt_block_date
  - proxy
  - singleton
```

This ensures consistency across all networks and prevents compilation errors.
