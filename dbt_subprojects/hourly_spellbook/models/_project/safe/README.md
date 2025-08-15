# Safe Project Documentation

This documentation covers the Safe (formerly Gnosis Safe) spell implementation in the Dune spellbook, including data models, validation, and configuration.

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

## Singleton Validation

All singleton addresses discovered from proxy factories are validated against official Safe deployments to ensure data integrity and prevent tracking of unofficial or malicious deployments.

### Official Safe Singleton Addresses

The following are the official Safe singleton addresses that are validated across all networks:

```
v1.0.0 - Safe:             0xb6029EA3B2c51D09a50B53CA8012FeEB05bDa35A
v1.1.1 - Safe:             0x34CfAC646f301356fAa8B21e94227e3583Fe3F5F
v1.2.0 - Safe:             0x6851D6fDFAfD08c0295C392436245E5bc78B0185
v1.3.0 - Safe:             0xd9Db270c1B5E3Bd161E8c8503c55cEABeE709552
v1.3.0 - Safe (EIP-155):   0x69f4D1788e39c87893C980c06EdF4b7f686e2938
v1.3.0 - Safe (ZKSync):    0xB00ce5CCcdEf57e539ddcEd01DF43a13855d9910
v1.3.0 - SafeL2:           0x3E5c63644E683549055b9Be8653de26E0B4CD36E
v1.3.0 - SafeL2 (EIP-155): 0xfb1bffC9d739B8D520DaF37dF666da4C687191EA
v1.3.0 - SafeL2 (ZKSync):  0x1727c2c531cf966f902E5927b98490fDFb3b2b70
v1.4.1 - Safe:             0x41675C099F32341bf84BFc5382aF534df5C7461a
v1.4.1 - Safe (ZKSync):    0xC35F063962328aC65cED5D4c3fC5dEf8dec68dFa
v1.4.1 - SafeL2:           0x29fcB43b46531BcA003ddC8FCB67FFE91900C762
v1.4.1 - SafeL2 (ZKSync):  0x610fcA2e0279Fa1F8C00c8c2F71dF522AD469380
v1.5.0 - Safe:             0xFf51A5898e281Db6DfC7855790607438dF2ca44b
v1.5.0 - SafeL2:           0xEdd160fEBBD92E350D4D398fb636302fccd67C7e
```

Note: Some versions have multiple addresses due to different deployment methods (standard, EIP-155 chain-specific, and zkSync).

Source: https://github.com/safe-global/safe-deployments

### Validation Implementation

- All singleton models use the `safe_singletons_by_network_validated()` macro with `only_official=true`
- This filters discovered singletons to only include the official addresses listed above
- Unofficial or unknown singleton addresses are automatically excluded from the data
- The `safe_singleton_validation` model provides cross-chain analytics on singleton deployments

## Network Configuration

Networks are configured in `macros/project/safe/safe_network_config.sql`. Each network configuration includes:
- `start_date`: When Safe was deployed on that network
- `native_token`: The native token of the network (ETH, BNB, MATIC, etc.)
- `singleton_type`: Type of singleton implementation
- `singleton_sources`: List of ProxyFactory event tables to use

### Singleton Types

Networks are categorized by their singleton implementation type:

1. `modern`: Networks that only have SafeProxyFactory v1.3.0 and/or v1.4.1 events
   - Examples: Arbitrum, Base, Optimism, most newer networks
   
2. `legacy`: Networks that have both old ProxyFactory v1.1.1 tables and newer SafeProxyFactory tables
   - Networks: BNB, Gnosis, Polygon
   - These require combining data from multiple factory versions
   
3. `legacy_ethereum`: Special configuration for Ethereum mainnet
   - Has unique table structure from early Safe deployments
   - Includes ProxyFactory v1.0.0, v1.1.0, and v1.1.1 implementations

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

## Contributor Management

All Safe project contributors are managed centrally through the `get_safe_contributors()` macro in `safe_network_config.sql`. This ensures consistent attribution across all models without needing to update individual files.

Current contributors:
- tschubotz
- peterrliem
- danielpartida
- hosuke
- frankmaseo
- kryptaki
- sche
- safehjc

To add or update contributors, modify the `default_contributors` list in the `get_safe_contributors()` macro.

## Adding a New Network

To add support for a new network:

1. Add network configuration to `macros/project/safe/safe_network_config.sql`
   - Set appropriate `singleton_type` based on available tables
   - Configure `start_date` and `native_token`
   - Add singleton sources based on what's available
2. Create source definitions in `sources/safe/<network>/safe_<network>_sources.yml`
   - Follow the naming conventions exactly (e.g., `SafeProxyFactory_v1_3_0_evt_ProxyCreation`)
   - Include all required columns as defined in the Column Structure section
3. Create model files in `models/_project/safe/<network>/`
   - Use the centralized configuration macros (`safe_config`, `safe_table_config`)
   - Models will automatically use validated singleton filtering
4. Add schema configuration to `dbt_project.yml`
   - Add entry under `models.hourly_spellbook._project.safe.<network>`
5. Update aggregation models to include the new network
   - Add to `safe_safes_all.sql`, `safe_transactions_all.sql`, etc.
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

## Macro Organization

The Safe project uses several macros for configuration and logic, located in `macros/project/safe/`:

### Configuration Macros
- `safe_network_config.sql`: Central network configuration and contributor management
  - `get_safe_network_config()`: Returns configuration for a specific network
  - `get_safe_contributors()`: Returns the list of contributors
- `safe_config.sql`: Model configuration helpers
  - `safe_config()`: Configuration for incremental models
  - `safe_table_config()`: Configuration for table models (e.g., singletons)

### Logic Macros
- `safe_singletons.sql`: Singleton discovery logic
  - `safe_singletons_modern()`: For networks with only modern proxy factories
  - `safe_singletons_legacy()`: For networks with both legacy and modern factories
  - `safe_singletons_ethereum()`: Special logic for Ethereum mainnet
  - `safe_singletons_by_network()`: Router macro that selects appropriate logic

### Validation Macros
- `safe_singletons_validated.sql`: Validated singleton discovery
  - `get_official_safe_deployments()`: Single source of truth for all official Safe addresses and versions
  - `get_official_safe_addresses()`: Returns list of official Safe addresses (15 addresses marked for validation)
  - `safe_singletons_by_network_validated()`: Main macro for validated singleton discovery
  - Filters to only include official Safe deployments

### Other Macros
- `safe_safes.sql`: Safe creation logic with version detection
- `safe_transactions.sql`: Transaction extraction and processing
- `safe_utils.sql`: Utility functions and wrapper macros for Safe models

## CI/CD Optimization

All Safe macros support a `date_filter` parameter to constrain data to the last 7 days for CI/CD pipeline performance:
- Set `date_filter=true` in model files to enable filtering
- This prevents timeouts during automated testing
- Production runs use full historical data when `date_filter=false` (default)

## Best Practices

### Address Comparisons
All address comparisons in the Safe project use case-insensitive matching with `LOWER()`:
- Always wrap addresses in `LOWER()` function for comparisons
- Example: `LOWER(address) = LOWER('0x...')`
- This prevents issues with checksummed vs non-checksummed addresses

### Centralized Configuration
- Project start dates are managed centrally in `safe_network_config.sql`
- Official Safe addresses are managed in `get_official_safe_deployments()` macro
- Contributors are managed in `get_safe_contributors()` macro
- Never hardcode these values in individual models
