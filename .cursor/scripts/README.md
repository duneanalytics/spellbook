# Dune Scripts

Utilities for querying Dune and checking chain support. Scripts use [inline dependency declarations](https://docs.astral.sh/uv/guides/scripts/#declaring-script-dependencies).

## Scripts

### dune_query.py
Run Dune queries by ID or SQL string.

```bash
uv run dune_query.py --query-id 3493826
uv run dune_query.py --query-id 6293737 --param chain=xlayer
uv run dune_query.py --query "SELECT * FROM dex.trades LIMIT 10"
```

### check_amp_support.py
Check if a chain has AMP metadata API support.

```bash
uv run check_amp_support.py <chain_name>
```

## Environment

Set in `.env` at project root:
```
DUNE_API_KEY=your_api_key
SIM_METADATA_API_KEY=your_sim_key
```
