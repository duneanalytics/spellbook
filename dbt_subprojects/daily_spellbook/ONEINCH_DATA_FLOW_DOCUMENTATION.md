# 1inch Spellbook Data Flow and Architecture Documentation

## Overview

The 1inch Spellbook project is a sophisticated multi-chain DEX analytics system built on dbt (data build tool). It processes swap transactions across 13 different blockchains, tracking various protocols including the 1inch aggregator, limit order protocol, and multiple DEX integrations.

## Architecture Components

### 1. Macro Layer (`macros/project/oneinch/`)

The macro layer provides reusable SQL generation logic that is blockchain-agnostic:

- **Configuration Macros** (`_meta/`):
  - `oneinch_mapped_contracts_macro`: Maps contract addresses to protocols with metadata
  - `oneinch_project_swaps_exposed_blockchains_list`: Maintains list of supported blockchains

- **Data Processing Macros** (`project/`):
  - `oneinch_project_swaps_macro`: Main swap processing logic
  - `oneinch_project_orders_macro`: Order matching and enrichment
  - `oneinch_project_calls_macro`: Protocol call identification
  - `oneinch_project_ptfc_macro`: Token transfer processing
  - `oneinch_project_orders_raw_logs_macro`: Raw event log extraction
  - `oneinch_project_orders_raw_traces_macro`: Raw trace extraction

- **Configuration Macros** (`project/cfg/`):
  - `oneinch_project_orders_cfg_events_macro`: Event parsing configuration
  - `oneinch_project_orders_cfg_methods_macro`: Method parsing configuration

### 2. Model Layer (`models/_projects/oneinch/`)

The model layer implements the macros for each blockchain and aggregates cross-chain data:

- **Cross-chain Models** (root level):
  - `oneinch_project_swaps`: Aggregates all blockchain swap data
  - `oneinch_project_orders`: Aggregates all blockchain order data
  - `oneinch_mapped_contracts`: Cross-chain contract registry

- **Blockchain-specific Models** (`[blockchain]/`):
  - Each blockchain has identical model structure
  - Models call macros with blockchain parameter
  - Incremental materialization for efficiency

## Data Flow Pipeline

### Stage 1: Contract Mapping
```
Raw contract data → oneinch_mapped_contracts_macro → oneinch_[blockchain]_mapped_contracts
                                                   ↓
                                           oneinch_mapped_contracts (cross-chain)
```

**Purpose**: Establishes which contracts belong to which protocols and their metadata.

### Stage 2: Raw Data Extraction
```
Blockchain logs → oneinch_project_orders_raw_logs_macro → oneinch_[blockchain]_project_orders_raw_logs
                                                        ↓
Blockchain traces → oneinch_project_orders_raw_traces_macro → oneinch_[blockchain]_project_orders_raw_traces
```

**Purpose**: Extracts relevant events and method calls from blockchain data.

### Stage 3: Call Processing
```
Traces + Mapped Contracts → oneinch_project_calls_macro → oneinch_[blockchain]_project_calls
```

**Purpose**: Identifies swap-related method calls across all protocols.

### Stage 4: Order Processing
```
Raw Logs + Raw Traces → oneinch_project_orders_macro → oneinch_[blockchain]_project_orders
                                                     ↓
                                             oneinch_project_orders (cross-chain)
```

**Purpose**: Matches logs with traces to create complete order records.

### Stage 5: Swap Assembly
```
Orders + Calls + Transfers → oneinch_project_swaps_macro → oneinch_[blockchain]_project_swaps
                                                         ↓
                                                 oneinch_project_swaps (cross-chain)
```

**Purpose**: Combines all data sources to create comprehensive swap records with:
- Token amounts and USD values
- User addresses and transaction details
- Swap classification (direct, intent, cross-chain)
- Protocol and contract information

## Key Data Transformations

### 1. Contract Address Mapping
- Enriches addresses with project/protocol information
- Adds creation metadata and flags (user, multi, recreated, cross_chain)
- Filters by blockchain

### 2. Event and Method Parsing
- Uses configuration to parse different event/method formats
- Extracts order parameters (maker, taker, amounts, tokens)
- Handles protocol-specific nuances

### 3. Order Matching
- Joins logs and traces on order hash/salt
- Calculates dynamic amounts for time-based orders
- Handles partial fills and order states

### 4. Transfer Processing
- Identifies token transfers related to swaps
- Handles native token transfers
- Special logic for wrapper deposits/withdrawals

### 5. Swap Classification
- **Intra-chain Classic**: Direct swaps through aggregator
- **Intra-chain Intents**: Auction-based or limit orders
- **Cross-chain**: Swaps involving multiple blockchains

### 6. Price Calculation
- Joins with price feeds for USD valuation
- Handles missing prices gracefully
- Calculates both token amounts and USD values

## Optimization Opportunities

### 1. Multi-stage Processing Enhancement

**Current Issue**: The main swap macro performs multiple heavy operations in a single query.

**Suggestion**: Break down `oneinch_project_swaps_macro` into smaller stages:
```sql
-- Stage 1: Basic swap assembly
create table oneinch_[blockchain]_swaps_stage1 as ...

-- Stage 2: Price enrichment
create table oneinch_[blockchain]_swaps_stage2 as ...

-- Stage 3: Final classification and aggregation
create table oneinch_[blockchain]_project_swaps as ...
```

**Benefits**:
- Better query optimization
- Easier debugging and monitoring
- Potential for parallel processing

### 2. Caching Frequently Used Data

**Current Issue**: Mapped contracts are joined repeatedly.

**Suggestion**: Create materialized intermediate tables for:
- Active contract sets per blockchain
- Common token pairs
- Frequent trader addresses

### 3. Partitioning Strategy Optimization

**Current State**:
- Orders: Monthly partitions
- Raw data: Daily partitions
- Swaps: Monthly + project partitions

**Suggestion**: Align partitioning for better join performance:
- Use consistent partition granularity
- Consider hash partitioning for large tables
- Add partition pruning hints

### 4. Incremental Processing Improvements

**Current Issue**: Full table scans for some joins.

**Suggestion**:
```sql
-- Add time-based filters earlier in CTEs
with filtered_transfers as (
    select * from transfers
    where block_time >= (select max(block_time) - interval '1 day' from {{ this }})
)
```

### 5. Protocol-specific Models

**Current Issue**: All protocols processed together.

**Suggestion**: Create protocol-specific models for heavy protocols:
- `oneinch_[blockchain]_aggregator_swaps`
- `oneinch_[blockchain]_lop_swaps`
- Aggregate in final step

**Benefits**:
- Parallel processing
- Protocol-specific optimizations
- Easier maintenance

### 6. Configuration as Data

**Current Issue**: Configuration embedded in macros.

**Suggestion**: Move to seed files:
```yaml
# seeds/oneinch_event_config.yml
events:
  - topic0: "0x..."
    parser: "orderFilled"
    version: "v5"
```

**Benefits**:
- Version control for configurations
- Easier updates without code changes
- Potential for dynamic configuration

### 7. Monitoring and Alerting

**Suggestion**: Add data quality checks:
```sql
-- Add to each model
{{ test_not_null(['block_time', 'tx_hash']) }}
{{ test_unique(['blockchain', 'tx_hash', 'evt_index']) }}
{{ test_relationships('token_bought', 'tokens.contract_address') }}
```

### 8. Query Performance Optimization

**Specific Improvements**:
1. Use `UNION ALL` instead of `UNION` where duplicates are impossible
2. Add index hints for large joins
3. Use approximate functions for non-critical aggregations
4. Implement query result caching for stable historical data

### 9. Resource Management

**Suggestion**: Add model-specific resource configurations:
```yaml
config:
  # For heavy models
  query_tag: 'oneinch_heavy_processing'
  warehouse_size: 'xl'
  timeout: 7200
```

## Maintenance Considerations

1. **Adding New Blockchains**:
   - Add to `oneinch_project_swaps_exposed_blockchains_list`
   - Create blockchain folder with standard model set
   - Update cross-chain aggregation models

2. **Adding New Protocols**:
   - Update contract mapping configuration
   - Add event/method parsing configuration
   - Test with sample transactions

3. **Performance Monitoring**:
   - Track model run times
   - Monitor data freshness
   - Set up alerts for anomalies

## Conclusion

The 1inch Spellbook architecture demonstrates sophisticated multi-chain data processing with good separation of concerns. The macro-based approach ensures consistency across blockchains while allowing flexibility. The suggested optimizations focus on breaking down complex operations, improving caching, and enabling better parallel processing to handle the growing data volume efficiently.