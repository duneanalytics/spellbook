# Performance Test Models

This directory contains test models for comparing performance between different query execution approaches for `dex.trades`.

## Test Models

These models represent the 5 different query execution approaches analyzed in the performance study:

1. **`test_run1_materialized_only.sql`** - Materialized `dex.trades` only
   - Query Structure: `FROM dex.trades` → `JOIN dex.trades`
   - Expected: 63 stages, 52.51s elapsed, 10.01m CPU, 9.53GB memory

2. **`test_run2_view_table.sql`** - View + Table (hybrid) ⭐ **BEST OVERALL**
   - Query Structure: `FROM dex.trades_view_test` → `JOIN dex.trades`
   - Expected: 815 stages, 45.09s elapsed, 6.17m CPU, 4.49GB memory
   - **Best balance**: Cheapest for Dune (38% less CPU, 53% less memory) AND better for users (14% faster)

3. **`test_run3_table_view.sql`** - Table + View (reverse) ⚡ **FASTEST**
   - Query Structure: `FROM dex.trades` → `JOIN dex.trades_view_test`
   - Expected: 487 stages, 39.69s elapsed, 9.49m CPU, 9.75GB memory
   - **Trade-off**: Fastest for users (24% faster) but more expensive (5% more CPU, 2% more memory)

4. **`test_run4_views_only.sql`** - Views only (2x) ❌ **FAILS**
   - Query Structure: `FROM dex.trades_view_test` → `JOIN dex.trades_view_test`
   - Expected: 1,239 estimated stages - **EXCEEDS 1,000 STAGE LIMIT**
   - Status: Query fails during planning phase

5. **`test_run5_views_with_mat.sql`** - Views only (2x, with as_is materialized) ❌ **FAILS**
   - Query Structure: `FROM dex.trades_view_test_with_mat_only` → `JOIN dex.trades_view_test_with_mat_only`
   - Expected: 1,239 estimated stages - **EXCEEDS 1,000 STAGE LIMIT** (same as Run 4!)
   - Status: Query fails during planning phase
   - **Key Finding**: Materializing as_is DEXes does NOT reduce stage count

## Usage

These models are tagged with `performance_test` and can be run in CI/CD pipelines to:
- Compare performance metrics (stages, CPU time, memory usage)
- Validate that Run 2 continues to be the optimal approach
- Monitor for performance regressions

## Running Tests

```bash
# Run all performance tests
dbt run --select tag:performance_test

# Run specific test
dbt run --select test_run2_view_table

# Run with performance profiling
dbt run --select tag:performance_test --profiles-dir ~/.dbt
```

## Expected Results

Based on the performance analysis:
- **Run 1**: Baseline performance, most expensive
- **Run 2**: Best overall (recommended) ✅
- **Run 3**: Fastest but more expensive
- **Run 4**: Will fail due to stage limit
- **Run 5**: Will fail due to stage limit (materialization doesn't help)

## Reference

See `trino_logs/PERFORMANCE_ANALYSIS.md` for detailed analysis and findings.

