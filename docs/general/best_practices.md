# Best Practices

## Development Workflow

### 1. Start in the Dune App
Always begin with a working query in the Dune web application before creating a dbt model. The Dune app provides immediate feedback and is easier to debug than CI runs.

### 2. Convert to a dbt Model
Once your query is working correctly, convert it to a dbt model with the appropriate materialization:
- **`view`** – No data stored, query runs each execution. Best for lightweight transformations.
- **`table`** – Full refresh every run (frequency depends on sub-project). Best for medium-sized datasets and dimension tables.
- **`incremental`** – Only adds/updates recent rows. Best for large fact tables and event streams. Requires `incremental_strategy`, `file_format='delta'`, and `unique_key`.

Always use `source()` and `ref()` — never hardcode table names.

### 3. Compile and Test Locally
Run `dbt compile` in the relevant sub-project directory (e.g., `dbt_subprojects/dex/`). This outputs raw SQL in the `target/` directory to copy/paste into Dune, or use the `dune_query.py` script:
```bash
python scripts/dune_query.py "@model_name" --limit 100
```

### 4. Use Temporary Hardcoded Date Filters for Initial CI Runs
Hardcode a short date filter (e.g., last 3-7 days) on large source tables during initial development. This speeds up CI GitHub Action runs so you can verify end-to-end success quickly.

### 5. Submit PR and Use CI
Each commit to your feature branch triggers CI, which builds and tests all modified models. CI tables can be queried on Dune for ~24 hours (format: `test_schema.git_dunesql_<hash>_<table>`). Leverage these tables for QA testing — or even full test dashboards.

### 6. Revert Hardcoded Filters Before Merge
**Always revert hardcoded date filters before merge and deployment.** The final model must use the standard `incremental_predicate()` macro — not hardcoded dates. Historical backfill is handled via `--full-refresh` post-merge, not by widening filters in model logic.

## Incremental Model Setup

- Unique key columns must be *exactly* the same in the model config block, schema yml file, and seed match columns (where applicable)
- There cannot be NULLs in the unique key columns — in Trino, NULLs cause merge lookups to fail, leading to duplicates
    - Use `coalesce()` on key columns, or `dbt_utils.generate_surrogate_key()` if columns may contain NULLs
- Always use the `incremental_predicate()` macro for `incremental_predicates` rather than hardcoding:
    ```sql
    incremental_predicates=[incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    ```
- Only use `incremental_predicates` for time-series data — do NOT use when you need to check against full history (e.g., pool creation events)

## Performance Tips

### Join Ordering
Larger tables should be on the left side of joins. The DuneSQL query planner usually handles this, but when spells run longer than expected, check join order.

When joining on partition columns, include time filters in **both** the ON clause and the WHERE clause to enable partition pruning on both sides.

### Time-Based Filtering
Always filter by partition columns (`block_date`, `block_time`, `evt_block_time`) to enable partition pruning. Cross-chain tables (like `dex.trades`, `tokens.transfers`) are partitioned by **both** `blockchain` and time — always specify both filters.

### Select Only Required Columns
Never use `SELECT *` on large tables. Dune's columnar storage makes column selection especially effective for transaction, log, and trace tables.

### UNION Statements
Use `UNION ALL` when there are no duplicates, `UNION DISTINCT` when deduplication is needed. Avoid bare `UNION`.

### ORDER BY
Never use `ORDER BY` without `LIMIT` on large result sets. Sorting is expensive — only use it when you need top-N results.

## DuneSQL Data Types & Functions
For best performance, maintain native DuneSQL data types rather than casting:
- `varbinary` — addresses, hashes. Use hex literals without quotes: `0x039e...` not `'0x039e...'`
- `uint256 / int256` — large numbers

When working with these types, leverage the native functions rather than casting:
- `bytearray_substring()`, `bytearray_to_uint256()`, `bytearray_length()`, etc.

## Partitioning

Partitioning is NOT always beneficial — only use it for large tables where each partition contains 1M+ rows.

Common patterns:
- `partition_by=['block_month']` — most common (trades, transfers, swaps)
- `partition_by=['block_date']` — very high-volume tables
- `partition_by=['blockchain', 'project', 'block_month']` — cross-chain sector spells

**If a table is partitioned, always include the partition column(s) in `unique_key`** — this enables Trino to prune partitions during merge lookups, dramatically improving performance.

## Leverage Jinja Syntax
Where possible, apply Jinja syntax:
- `source()` & `ref()` for all table references
- For loops to iterate through chains or project versions in union models
- Variables and macros for reusable logic
- For more information: https://docs.getdbt.com/docs/build/jinja-macros#jinja
