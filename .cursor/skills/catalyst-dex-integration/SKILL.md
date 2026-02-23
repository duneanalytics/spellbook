---
name: catalyst-dex-integration
description: Add a DEX project to dex.trades for a chain (sources, base trades, chain unions, seed, dex_info)
disable-model-invocation: true
---
# catalyst dex integration

## overview
Adds DEX abstractions for requested decentralized exchanges to `dex.trades`.

**When to use:** Use when the user wants to add one or more DEXs (by project/namespace) to dex.trades for a given chain.

## parameters
- `<issue_id>`: linear issue id (e.g., CUR2-548)
- `<chain>`: chain name (e.g., monad, sonic)
- `<project>`: DEX project name (e.g., kuru, uniswap)
- `<namespace>`: contract namespace for source lookup (partial string ok, e.g., "kuru", "uni")

From the invocation or user message: 1st = issue_id, 2nd = chain, 3rd = project, 4th = namespace. Use these for every `<placeholder>` below. If given in free form, infer or ask once for missing values.

## usage
```
/catalyst-dex-integration CUR2-548 monad kuru kuru
```

Dune MCP: server `user-dune-mcp`; tools `query_sql`, `run_query_by_id`. Use parameters as shown.

## conventions
- **Execution order:** Numbered items = execute sequentially. Any step that says "run" or "execute" is blocking; complete it before proceeding.
- **Code patterns:** Use existing chain patterns as reference (e.g. `dbt_subprojects/dex/models/trades/kaia/`, `.../mezo/`). Ordering: mimic existing; if unclear, append. Swap chain name in: file paths, model names, schema entries, `blockchain` values.
- **Contributors:** New files: set git username only. Existing files: append git username.

## prep vars
- Retrieve chain metadata: use Dune MCP **query_sql** with query: `select * from dune.blockchains where name = '<chain>'` (substitute `<chain>` with the chain name). Extract: `chain_id`, `name` (display name), `token_address` (native token).
- Retrieve first_block_time: use Dune MCP **query_sql** with query: `select min(time) from <chain>.blocks where number <> 0` (substitute `<chain>`).

## git workflow
1. **Verify main is up to date:** fetch latest, pull if behind, exit if diverged.
2. **Create branch:** name `<issue_id>-<chain>-dex-integration`, create off `main`, checkout, warn if exists. Don't commit/push anything.

## additional prep
- Verify decoded DEX tables exist: use Dune MCP **run_query_by_id** with `query_id: 6318398`, `query_parameters: '{"chain":"<chain>","namespace":"<namespace>"}'` (substitute `<chain>` and `<namespace>`). Retrieve from query results: `namespace`, `name` and `abi`.
- Find common events in `abi` following patterns like: Swap, PairCreated, PoolCreated, etc. If not found, query `<chain>.logs_decoded`.
- Identify DEX type: uniswap v2 fork, v3 fork, or custom.

## steps
1. **define DEX sources**
   - create/update `sources/_sector/dex/trades/<chain>/_sources.yml`
   - append new sources replicating pattern from existing source blocks in that file (copy one block, replace chain/project/namespace)

2. **create platform base trades model**
   - create `dbt_subprojects/dex/models/trades/<chain>/platforms/<project>_<chain>_base_trades.sql`
   - uniswap v2 forks: use `uniswap_compatible_v2_trades` macro
   - uniswap v3 forks: use `uniswap_compatible_v3_trades` macro
   - custom DEXs: check docs; use as reference `dbt_subprojects/dex/models/trades/<chain>/platforms/kuru_monad_base_trades.sql` or existing macros

3. **chain-level setup** (new chain only)
   - create `dbt_subprojects/dex/models/trades/<chain>/dex_<chain>_base_trades.sql`
   - create `dbt_subprojects/dex/models/trades/<chain>/dex_<chain>_trades.sql`
   - create `dbt_subprojects/dex/models/trades/<chain>/dex_<chain>_token_volumes_daily.sql`
   - add `<chain>` to chains list in `dbt_subprojects/dex/models/trades/dex_trades.sql`
   - add `<chain>` to chains list in `dbt_subprojects/dex/models/trades/dex_token_volumes_daily.sql`

4. **create/update schema file**
   - create `dbt_subprojects/dex/models/trades/<chain>/_schema.yml` (new chain) with `dex_<chain>_trades`, `dex_<chain>_base_trades`, `dex_<chain>_token_volumes_daily`
   - append platform model definition to existing schema

5. **create seed file**
   - append `<project>_<chain>_base_trades_seed` to `dbt_subprojects/dex/seeds/trades/_schema.yml`
   - create empty `dbt_subprojects/dex/seeds/trades/<project>_<chain>_base_trades_seed.csv`

6. **update dex_info.sql**
   - if not already present, append `<project>` to `dbt_subprojects/dex/models/dex_info.sql`

7. **final checks**
   - From repo root: run `pipenv shell`, then `cd dbt_subprojects/dex` and `dbt compile` (or `dbt compile --select <project>_<chain>_base_trades`). Fix any errors.
   - **Populate seed csv:** Replace `<COMPILED_BASE_TRADES_SQL>` in the query below with the exact compiled SQL for the model `<project>_<chain>_base_trades` (from `dbt compile` output). Use it as a subquery, not a table name. Run the full query via Dune MCP **query_sql** and paste the 2–3 rows into the seed CSV.
   ```sql
   with base_trades as (
     select
       blockchain, project, version, block_date, tx_hash, evt_index,
       token_bought_address, token_sold_address, block_number,
       token_bought_amount_raw, token_sold_amount_raw,
       row_number() over (partition by blockchain, project, version order by block_number desc) as rn
     from ( <COMPILED_BASE_TRADES_SQL> )
   )
   select
     blockchain, project, version, block_date, tx_hash, evt_index,
     token_bought_address, token_sold_address, block_number,
     token_bought_amount_raw, token_sold_amount_raw
   from base_trades where rn <= 3
   ```

## reference examples
- custom DEX: `dbt_subprojects/dex/models/trades/<chain>/platforms/kuru_monad_base_trades.sql`
- uniswap v2 fork: `dbt_subprojects/dex/models/trades/<chain>/platforms/uniswap_v2_monad_base_trades.sql`
- uniswap v3 fork: `dbt_subprojects/dex/models/trades/<chain>/platforms/uniswap_v3_monad_base_trades.sql`

## notes
- for multi-version DEXs (v2 + v3), create separate models
- use version filter in seed test if sharing seed file
