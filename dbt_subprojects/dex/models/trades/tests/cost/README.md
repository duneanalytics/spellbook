# Cost Test Models

This directory contains test models for comparing query cost between different table/view approaches for staking asset analysis.

## Test Models

These models represent cost comparisons using different table/view sources:

1. **`test_cost_dex_trades.sql`** - Using materialized `dex.trades`
   - Source: `{{ ref('dex_trades') }}`
   - Baseline cost comparison

2. **`test_cost_dex_trades_view_test.sql`** - Using `dex.trades_view_test` (view)
   - Source: `{{ ref('dex_trades_view_test') }}`
   - Expected: Better cost performance due to predicate pushdown when filters are applied

3. **`test_cost_dex_trades_view_test_with_mat.sql`** - Using `dex.trades_view_test_with_mat_only` (view with materialized as_is DEXes)
   - Source: `{{ ref('dex_trades_view_test_with_mat_only') }}`
   - Note: Uses materialized as_is DEXes (oneinch_lop_own_trades_mat, zeroex_native_trades_mat)
   - Expected: Similar cost to dex.trades_view_test as materialization of as_is components doesn't significantly impact cost

## Query Structure

All tests use the same query structure based on [Dune Query #6244496](https://dune.com/queries/6244496):
- Joins `dex.trades` (or view variants) with `dune.ether_fi.result_traits_staking_assets`
- Filters for `block_date >= '2023-01-01'` and excludes '1inch Lop' project
- Unions token_bought and token_sold records
- Measures cost of querying different table/view sources

## Usage

```bash
# Run all cost tests
dbt run --select tag:cost_test

# Run specific cost test
dbt run --select test_cost_dex_trades_view_test

# Compare costs across all three approaches
dbt run --select tag:cost_test --profiles-dir ~/.dbt
```

## Expected Results

Based on performance analysis:
- **dex.trades**: Baseline cost (materialized table)
- **dex.trades_view_test**: Potentially lower cost with filters (predicate pushdown)
- **dex.trades_view_test_with_mat_only**: Similar cost to view (materialization of components doesn't help significantly)

## Reference

See `trino_logs/PERFORMANCE_ANALYSIS.md` for detailed analysis of cost implications.

