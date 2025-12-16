# Dune Scripts

Utilities for querying Dune and checking chain support.

## Setup

```bash
cd .cursor/scripts
uv sync
```

## Scripts

### dune_query.py
Run Dune queries by ID or SQL string.

```bash
uv run python dune_query.py --query-id 3493826
uv run python dune_query.py --query-id 6293737 --param chain=xlayer
uv run python dune_query.py --query "SELECT * FROM dex.trades LIMIT 10"
```

### check_amp_support.py
Check if a chain has AMP metadata API support.

```bash
uv run python check_amp_support.py <chain_name>
```

## Environment

Set in `.env` at project root:
```
DUNE_API_KEY=your_api_key
SIM_METADATA_API_KEY=your_sim_key
```
