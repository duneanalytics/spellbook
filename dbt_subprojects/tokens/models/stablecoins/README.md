# Stablecoin Transfers Models

## Overview

This directory contains models for tracking stablecoin-specific token transfers across different blockchains. These models are subsets of the general `tokens_<chain>_transfers` models, filtered to only include stablecoin tokens.

## Models

### `stablecoin_ethereum_transfers`

Filters Ethereum token transfers to only include stablecoins defined in `tokens_ethereum_erc20_stablecoins`.

**Features:**
- All standard token transfer columns (tx_hash, from, to, amount, etc.)
- Enhanced with stablecoin-specific metadata:
  - `backing`: Type of stablecoin backing (Fiat-backed, Crypto-backed, Algorithmic, Hybrid, RWA-backed)
  - `stablecoin_name`: Full name/issuer of the stablecoin
  - `denomination`: Currency denomination (USD, EUR, GBP, etc.)
- Incremental materialization with partitioning by `block_month`
- Unique key on `[block_date, unique_key]`

**Schema:** `stablecoin_ethereum.transfers`

## Data Lineage

```
tokens_ethereum_erc20_stablecoins (seed/static list)
                |
                v
tokens_ethereum_base_transfers (raw ERC20 transfers)
                |
                v
    tokens_ethereum_transfers (enriched with prices, symbols)
                |
                v
 stablecoin_ethereum_transfers (filtered to stablecoins only)
```

## Testing

### Quick Validation Query

```sql
-- Check row count and date range
SELECT 
    COUNT(*) as total_transfers,
    COUNT(DISTINCT contract_address) as unique_stablecoins,
    MIN(block_date) as earliest_date,
    MAX(block_date) as latest_date
FROM stablecoin_ethereum.transfers
WHERE block_date >= DATE '2024-01-01';
```

### Top Stablecoins by Transfer Volume

```sql
-- Most active stablecoins
SELECT 
    symbol,
    backing,
    denomination,
    COUNT(*) as transfer_count,
    SUM(amount_usd) as total_volume_usd
FROM stablecoin_ethereum.transfers
WHERE block_date >= DATE '2024-01-01'
GROUP BY 1, 2, 3
ORDER BY total_volume_usd DESC
LIMIT 20;
```

### Verify Filtering Logic

```sql
-- Compare stablecoin transfers vs all token transfers
WITH stablecoin_count AS (
    SELECT COUNT(*) as stablecoin_transfers
    FROM stablecoin_ethereum.transfers
    WHERE block_date = DATE '2024-10-01'
),
all_tokens_count AS (
    SELECT COUNT(*) as all_transfers
    FROM tokens_ethereum.transfers t
    INNER JOIN tokens_ethereum.erc20_stablecoins s
        ON t.contract_address = s.contract_address
    WHERE t.block_date = DATE '2024-10-01'
)
SELECT 
    s.stablecoin_transfers,
    a.all_transfers,
    s.stablecoin_transfers = a.all_transfers as counts_match
FROM stablecoin_count s
CROSS JOIN all_tokens_count a;
```

### Test with Dune API

You can test queries using the `dune_query.py` utility:

```bash
# Test the model with a specific date range
python scripts/dune_query.py "
SELECT 
    symbol,
    backing,
    COUNT(*) as transfers,
    SUM(amount_usd) as volume_usd
FROM stablecoin_ethereum.transfers
WHERE block_date >= DATE '2024-10-01'
    AND block_date < DATE '2024-10-08'
GROUP BY 1, 2
ORDER BY volume_usd DESC
LIMIT 10
"

# Save results for analysis
python scripts/dune_query.py --sql-file test_query.sql --output results.csv
```

## Contributing

To add stablecoin transfers for a new chain:

1. Ensure `tokens_<chain>_erc20_stablecoins` exists with stablecoin addresses
2. Copy `stablecoin_ethereum_transfers.sql` and update:
   - Config schema to `stablecoin_<chain>`
   - Reference to `tokens_<chain>_transfers`
   - Reference to `tokens_<chain>_erc20_stablecoins`
3. Add model to `_schema.yml`
4. Test with sample queries
5. Update this README

## Schema

See [_schema.yml](./_schema.yml) for full column documentation.

