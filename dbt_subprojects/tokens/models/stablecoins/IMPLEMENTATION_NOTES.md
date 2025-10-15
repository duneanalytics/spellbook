# Stablecoin Ethereum Transfers Implementation Notes

## Summary

Created `stablecoin_ethereum_transfers` model as a filtered subset of `tokens_ethereum_transfers`, including only transfers of stablecoin tokens defined in `tokens_ethereum_erc20_stablecoins`.

## Files Created

1. **stablecoin_ethereum_transfers.sql** - Main model file
   - Schema: `stablecoin_ethereum.transfers`
   - Materialization: incremental (merge strategy)
   - Partitioning: by `block_month`
   - Unique key: `[block_date, unique_key]`
   - Additional columns: `backing`, `stablecoin_name`, `denomination`

2. **_schema.yml** - Model documentation and tests
   - Includes full column descriptions
   - Includes uniqueness test on `[block_date, unique_key]`

3. **README.md** - Comprehensive documentation
   - Model overview and features
   - Data lineage diagram
   - Testing queries
   - Contributing guidelines

4. **test_stablecoin_ethereum_transfers.sql** - Test query
   - Daily statistics
   - Stablecoin summary by symbol
   - Distribution by backing type
   - Distribution by denomination

## Design Decisions

### Why a Separate Model?

Instead of creating a flag column in the main `tokens_ethereum_transfers` model, we created a separate filtered model because:

1. **Performance**: Users analyzing stablecoins don't need to scan all token transfers
2. **Clarity**: Explicit model name makes intent clear (`stablecoin_ethereum.transfers` vs `tokens_ethereum.transfers WHERE is_stablecoin = true`)
3. **Enhanced Metadata**: Can include stablecoin-specific columns (backing, denomination) without cluttering the general tokens model
4. **Independent Updates**: Stablecoin list can be updated independently without affecting all token transfers
5. **Consistent Pattern**: Follows Dune's pattern of specialized subsets (similar to `dex_trades`, `nft_trades`, etc.)

### Data Flow

```
Raw Data (erc20_ethereum.evt_Transfer)
    ↓
tokens_ethereum_base_transfers (raw transfers)
    ↓
tokens_ethereum_transfers (enriched with prices, symbols)
    ↓
stablecoin_ethereum_transfers (filtered to stablecoins + metadata)
```

### Key Features

1. **Incremental Processing**: Uses same incremental logic as parent model
2. **Stablecoin Metadata**: Enriches transfers with backing type, issuer name, and denomination
3. **Comprehensive Coverage**: Includes ~110 stablecoin tokens on Ethereum
4. **Tested Schema**: Includes uniqueness tests and full documentation

## Testing Checklist

Before deployment:

- [x] Model file created with proper config
- [x] Schema documentation created
- [x] README with usage examples created
- [x] Test query file created
- [ ] Test with dune_query.py utility (requires test_schema name)
- [ ] Validate row counts match filtered tokens_ethereum_transfers
- [ ] Check incremental logic works correctly
- [ ] Verify stablecoin metadata joins correctly
- [ ] Compare specific transactions between test and prod

## Next Steps

1. **Get Test Schema Name**: Ask user for test_schema name
2. **Run Comparison Query**: Validate against tokens_ethereum_transfers
3. **Test Incremental Logic**: Verify incremental updates work
4. **Validate Sample Transactions**: Check known stablecoin transfers
5. **Create Dune Dashboard**: Build sample dashboard using the model

## Usage Examples

### Simple Query
```sql
SELECT * 
FROM stablecoin_ethereum.transfers
WHERE block_date >= DATE '2024-10-01'
LIMIT 100;
```

### Top Stablecoins by Volume
```sql
SELECT 
    symbol,
    backing,
    COUNT(*) as transfers,
    SUM(amount_usd) as volume_usd
FROM stablecoin_ethereum.transfers
WHERE block_date >= DATE '2024-10-01'
GROUP BY 1, 2
ORDER BY volume_usd DESC;
```

### Stablecoin Flows Analysis
```sql
SELECT 
    denomination,
    backing,
    DATE_TRUNC('day', block_time) as day,
    SUM(amount_usd) as daily_volume
FROM stablecoin_ethereum.transfers
WHERE block_date >= DATE '2024-01-01'
GROUP BY 1, 2, 3
ORDER BY day, daily_volume DESC;
```

## Potential Extensions

1. **Cross-chain Aggregation**: Create `stablecoin_transfers` model unioning all chains
2. **Additional Chains**: Replicate for other chains (Arbitrum, Optimism, Base, etc.)
3. **Net Transfers**: Create `stablecoin_ethereum_net_transfers_daily` model
4. **Balances**: Consider `stablecoin_ethereum_balances` if not already exists
5. **Exchange Flows**: Track flows to/from major exchanges
6. **Bridge Analysis**: Identify cross-chain bridge transfers

## Related Models

- `tokens_ethereum_transfers` - Parent model (all tokens)
- `tokens_ethereum_erc20_stablecoins` - Stablecoin address list
- `stablecoins_ethereum_balances` - Stablecoin balances (in daily_spellbook)

## Notes

- The model uses incremental materialization with merge strategy
- Partition pruning on `block_month` improves query performance
- The model follows Spellbook SQL best practices (explicit table aliases, proper data types)
- Post-hook includes `expose_spells` for proper cataloging

