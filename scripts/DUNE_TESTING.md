# 🌶️ Dune Testing with Spice

Comprehensive guide for testing query results against Dune using the `spice` library.

## Quick Start

1. **Install dependencies:**
```bash
pip install dune-spice polars python-dotenv
# or
pipenv install
```

2. **API key setup:**

The repo's `.env` file is automatically loaded. Ensure it contains:
```bash
DUNE_API_KEY=your-api-key-here
```

3. **Run a query:**
```bash
# Query by ID
python scripts/dune_query.py 21693

# Raw SQL
python scripts/dune_query.py "SELECT * FROM dex_solana.trades LIMIT 10"

# Compile and run dbt model
python scripts/dune_query.py "@uniswap_v3_unichain_base_trades"

# From file
python scripts/dune_query.py --sql-file path/to/query.sql
```

## Command Line Usage

```bash
# Query by ID or URL
python scripts/dune_query.py 21693
python scripts/dune_query.py https://dune.com/queries/21693

# Raw SQL
python scripts/dune_query.py "SELECT * FROM ethereum.blocks LIMIT 5"

# Compile and run dbt model
python scripts/dune_query.py "@uniswap_v3_unichain_base_trades"

# From SQL file
python scripts/dune_query.py --sql-file query.sql

# Force refresh (no cache)
python scripts/dune_query.py 21693 --refresh

# With parameters
python scripts/dune_query.py 21693 --params network=ethereum version=5

# Save results
python scripts/dune_query.py 21693 --output results.csv
python scripts/dune_query.py 21693 --output results.parquet

# Limit rows
python scripts/dune_query.py "SELECT * FROM ethereum.blocks" --limit 100

# Quiet mode
python scripts/dune_query.py 21693 --quiet
```

## Python Module Usage

```python
from scripts.dune_query import run_query, compare_dataframes, save_results

# Simple query
df = run_query("SELECT * FROM dex_solana.trades LIMIT 5")

# Compile and run dbt model
df = run_query("@uniswap_v3_unichain_base_trades")

# With parameters
df = run_query(21693, parameters={'network': 'ethereum'})

# Force refresh
df = run_query(21693, refresh=True)

# Compare test vs prod
test_df = run_query("SELECT * FROM test_schema.my_table ...")
prod_df = run_query("SELECT * FROM dex_solana.trades ...")
compare_dataframes(test_df, prod_df, "Test", "Production")

# Save results
save_results(df, "results.csv")
```

## API Reference

### `run_query(query, *, refresh=False, parameters=None, ...)`
Execute a Dune query and return polars DataFrame.

**Args:**
- `query`: Query ID, URL, raw SQL string, or dbt model name (with @ prefix)
- `refresh`: Force new execution (default: False)
- `parameters`: Dict of query parameters
- `api_key`: Dune API key (default: from .env)
- `performance`: 'medium' or 'large'
- `limit`: Max rows to return
- `cache`: Enable local caching (default: True)
- `compile_dbt`: Whether to compile dbt model before execution
- `project_dir`: dbt project directory (for dbt compilation)
- `profiles_dir`: dbt profiles directory (for dbt compilation)
- `target`: dbt target (for dbt compilation)

**Returns:** `polars.DataFrame`

### `compare_dataframes(df1, df2, label1='df1', label2='df2')`
Compare two DataFrames and print differences.

### `save_results(df, output_path, format=None)`
Save DataFrame to file. Format inferred from extension.

**Supported formats:** csv, parquet, json, xlsx

## Testing Workflows

### General testing workflow
```bash
# 1. Write comparison query
vim my_test_query.sql

# 2. Test it
python scripts/dune_query.py --sql-file my_test_query.sql

# 3. Save results
python scripts/dune_query.py --sql-file my_test_query.sql --output results.csv

# 4. Compare with baseline
python scripts/dune_query.py --sql-file baseline_query.sql --output baseline.csv
```

### Complex testing in Python
```python
from scripts.dune_query import run_query, compare_dataframes

# Query test schema (with your fix)
test_df = run_query("""
    SELECT * FROM test_schema.git_dunesql_xxx_dex_solana_trades
    WHERE project = 'raydium' AND version = 5
    LIMIT 1000
""")

# Query production (without fix)
prod_df = run_query("""
    SELECT * FROM dex_solana.trades  
    WHERE project = 'raydium' AND version = 5
    LIMIT 1000
""")

# Compare
compare_dataframes(test_df, prod_df, "Test (with fix)", "Production")

# Analyze
new_trades = test_df.shape[0] - prod_df.shape[0]
print(f"Fix recovered {new_trades} trades!")
```

## Features

- ✅ Simple CLI and Python API
- ✅ Query by ID, URL, raw SQL, or dbt models
- ✅ Automatic dbt model compilation and execution
- ✅ Auto-detection of dbt project directories
- ✅ Automatic caching for repeated queries
- ✅ Support for query parameters
- ✅ Multiple output formats (CSV, Parquet, JSON, Excel)
- ✅ DataFrame comparison utilities
- ✅ Auto-retry with exponential backoff
- ✅ Works with free Dune accounts

## Best Practices

1. **Start small**: Test with `LIMIT 100` before running full queries
2. **Use caching**: Results cached by default, use `--refresh` to force update
3. **Validate incrementally**: Test specific transactions → check row counts → full comparisons
4. **Save results**: Use `--output` for later analysis or sharing
5. **Use parameters**: Keep queries generic with `--params` instead of hardcoding
6. **Test dbt models**: Use `@model_name` syntax to compile and test models before deployment
7. **Auto-detection**: Let the script find the correct dbt project directory automatically

## Common Testing Scenarios

### Scenario 1: New feature adds rows
```bash
# Check if new implementation captures more data
python scripts/dune_query.py "
SELECT 
    'test' as source, COUNT(*) as cnt 
FROM test_schema.git_dunesql_4321313_dex_solana_trades
WHERE ...
UNION ALL
SELECT 
    'prod' as source, COUNT(*) as cnt
FROM dex_solana.trades
WHERE ...
"
```

### Scenario 2: Bug fix changes values
```bash
# Compare specific fields for known transactions
python scripts/dune_query.py --sql-file validate_values.sql
```

### Scenario 3: Performance optimization
```bash
# Same results, just faster
python scripts/dune_query.py "SELECT COUNT(*) FROM test_schema..." 
python scripts/dune_query.py "SELECT COUNT(*) FROM prod_schema..." 
# Should return same count
```

### Scenario 4: DBT Model Testing
```bash
# Test compiled dbt model
python scripts/dune_query.py "@uniswap_v3_unichain_base_trades" --limit 100

# Compare dbt model vs production
python scripts/dune_query.py "@uniswap_v3_unichain_base_trades" --limit 1000 --output dbt_results.csv
python scripts/dune_query.py "SELECT * FROM dex_unichain.trades WHERE project = 'uniswap' AND version = 3 LIMIT 1000" --output prod_results.csv

# Test different dbt models
python scripts/dune_query.py "@raydium_v5_base_trades" --limit 50
python scripts/dune_query.py "@balancer_v2_trades" --limit 50
```

## Troubleshooting

**No API key found:**
```bash
# Check .env file contains: DUNE_API_KEY=your-key
# Or set manually:
export DUNE_API_KEY="your-key"
python scripts/dune_query.py 21693 --api-key "your-key"
```

**Query timeout:**
- Add `LIMIT` to test queries
- Use `--performance large` for big queries
- Break into smaller date ranges

**Import errors:**
```bash
pip install dune-spice polars python-dotenv
```

**Results don't match expectations:**
- Verify test schema name is correct
- Check date ranges match
- Use `--refresh` to get latest data

**DBT compilation errors:**
- Ensure dbt is installed: `pipenv run dbt --version`
- Check model exists in expected project directory
- Use `--dbt-project-dir` to specify custom project path
- Verify dbt_project.yml exists in the project directory
- **Note:** The script automatically uses `pipenv run dbt` to avoid proto file conflicts with system dbt installations

**Proto file errors (types.proto duplicate):**
- This is a known issue with dbt-trino and system Python installations
- The `dune_query.py` script automatically uses pipenv's dbt to avoid this
- If you see this error elsewhere, use `pipenv run dbt` instead of `dbt` directly

## Tips & Tricks

- 💡 Use `--quiet` to suppress verbose output
- 💡 Chain queries: `python scripts/dune_query.py q1.sql && python scripts/dune_query.py q2.sql`
- 💡 Use parquet for speed: `--output results.parquet`
- 💡 Import in Jupyter for interactive analysis
- 💡 Use `git diff` on CSV files to track changes
- 💡 Test dbt models before deployment: `python scripts/dune_query.py "@model_name"`
- 💡 Auto-detection finds the right dbt project - no need to specify paths
- 💡 Use `@model_name` syntax for quick dbt model testing

## Next Steps

After validating your changes:
1. Review comparison results
2. Document expected differences
3. Get peer review
4. Deploy to production
5. Monitor for a few days

