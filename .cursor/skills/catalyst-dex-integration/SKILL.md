---
name: catalyst-dex-integration
description: Add a DEX project to dex.trades for a chain (sources, base trades, chain unions, seed, dex_info)
disable-model-invocation: true
---
# catalyst dex integration

## overview
adds dex abstractions for requested decentralized exchanges to `dex.trades`.

**when to use:** use when the user wants to add one or more dexs (by project/namespace) to `dex.trades` for a given chain.

## parameters
- `<issue_id>`: linear issue id (e.g., CUR2-548)
- `<chain>`: chain name (e.g., monad, sonic)
- `<project>`: DEX project name (e.g., kuru, uniswap)
- `<namespace>`: contract namespace for source lookup (partial string ok, e.g., "kuru", "uni")

from the invocation or user message: 1st = issue_id, 2nd = chain, 3rd = project, 4th = namespace. use these for every `<placeholder>` below. if given in free form, infer or ask once for missing values.

## usage
```
/catalyst-dex-integration CUR2-548 monad kuru kuru
```

dune mcp server: `user-dune-mcp`.
for ad-hoc sql, prefer temporary execution if the dune mcp supports it in-session. only create saved/throwaway queries with `createDuneQuery` when temporary execution is not available or when a later step explicitly needs a `query_id`.
fallback sequence when `query_id` is required: create query with `createDuneQuery` (pass sql in `query`) -> run with `executeQueryById` (using returned `query_id`) -> fetch rows with `getExecutionResults` (using returned `execution_id`).

## conventions
- **execution order:** numbered items = execute sequentially. any step that says "run" or "execute" is blocking; complete it before proceeding.
- **code patterns:** use existing chain patterns as reference (e.g. `dbt_subprojects/dex/models/trades/kaia/`, `.../mezo/`). ordering: mimic existing; if unclear, append. swap chain name in: file paths, model names, schema entries, `blockchain` values.
- **contributors:** new files: set git username only. existing files: append git username.

## prep vars
- retrieve chain metadata: run this sql via the ad-hoc sql sequence above: `select * from dune.blockchains where name = '<chain>'` (substitute `<chain>` with the chain name). extract: `chain_id`, `name` (display name), `token_address` (native token).
- retrieve first_block_time: run this sql via the ad-hoc sql sequence above: `select min(time) from <chain>.blocks where number <> 0` (substitute `<chain>`).

## git workflow
1. **verify `main` is up to date:** fetch latest, pull if behind, exit if diverged.
2. **create branch:** name `<issue_id>-<chain>-dex-integration`, create off `main`, checkout, warn if exists. don't commit/push anything.

## additional prep
- verify decoded dex tables exist: use dune mcp **executeQueryById** with `query_id: 6318398`, `query_parameters: [{"key":"chain","value":"<chain>","type":"text"},{"key":"namespace","value":"<namespace>","type":"text"}]` (substitute `<chain>` and `<namespace>`). retrieve from query results: `namespace`, `name`, and `abi`.
- find common events in `abi` following patterns like: `Swap`, `PairCreated`, and `PoolCreated`. if not found, query `<chain>.logs_decoded`.
- identify dex type: uniswap v2 fork, v3 fork, or custom.

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
   - **populate seed csv:** replace `<COMPILED_BASE_TRADES_SQL>` in the query below with the exact compiled sql for the model `<project>_<chain>_base_trades` (from `dbt compile` output). use it as a subquery, not a table name. run the full query via the ad-hoc sql sequence above and paste the 2-3 rows into the seed csv.
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
