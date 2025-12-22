# catalyst dex integration

## overview
adds DEX abstractions for requested decentralized exchanges to `dex.trades`

## parameters
- `<issue_id>`: linear issue id (e.g., CUR2-548)
- `<chain>`: chain name (e.g., monad, sonic)
- `<project>`: DEX project name (e.g., kuru, uniswap)
- `<namespace>`: contract namespace for source lookup (partial string ok, e.g., "kuru", "uni")

## usage
```
/catalyst/dex-integration CUR2-548 monad kuru kuru
```

## prerequisites
1. run `_shared.md#git-workflow` (task_suffix: `dex-integration`)
2. run `_shared.md#prep-vars`

additional prep:
- verify decoded DEX tables exist: `.cursor/scripts/dune_query.py --query-id 6318398 --param chain=<chain> --param namespace=<namespace>`
  - retrieve from query results: `namespace`, `name` and `abi`
- find common events in `abi` following patterns like: Swap, PairCreated, PoolCreated, etc.
  - if not found, query `<chain>.logs_decoded`
- identify DEX type: uniswap v2 fork, v3 fork, or custom

## steps
1. **define DEX sources**
  - create/update `sources/_sector/dex/trades/<chain>/_sources.yml`
  - append new sources replicating pattern from existing sources

2. **create platform base trades model**
  - create `dbt_subprojects/dex/models/trades/<chain>/platforms/<project>_<chain>_base_trades.sql`
  - uniswap v2 forks: use `uniswap_compatible_v2_trades` macro
  - uniswap v3 forks: use `uniswap_compatible_v3_trades` macro
  - custom DEXs: check docs, use existing macros/models as reference

3. **chain-level setup** (new chain only)
  - create `dbt_subprojects/dex/models/trades/<chain>/dex_<chain>_base_trades.sql`
  - add `ref('dex_<chain>_base_trades')` to `dbt_subprojects/dex/models/trades/dex_base_trades.sql`

4. **create/update schema file**
  - create `dbt_subprojects/dex/models/trades/<chain>/_schema.yml` (new chain)
  - or append platform model definition to existing schema

5. **create seed file**
  - append `<project>_<chain>_base_trades_seed` to `dbt_subprojects/dex/seeds/trades/_schema.yml`
  - create empty `dbt_subprojects/dex/seeds/trades/<project>_<chain>_base_trades_seed.csv`

6. **update dex_info.sql**
  - if not already present, append `<project>` to `dbt_subprojects/dex/models/dex_info.sql`

7. **final checks**
  - run `_shared.md#final-checks` (dex subproject)
  - run `dbt compile --select <project>_<chain>_base_trades`
  - populate seed csv with 2-3 sample trades:
    ```sql
    with base_trades as (
      select
        blockchain, project, version, block_date, tx_hash, evt_index,
        token_bought_address, token_sold_address, block_number,
        token_bought_amount_raw, token_sold_amount_raw,
        row_number() over (partition by blockchain, project, version order by block_number desc) as rn
      from <dbt_compiled_table>
    )
    select
      blockchain, project, version, block_date, tx_hash, evt_index,
      token_bought_address, token_sold_address, block_number,
      token_bought_amount_raw, token_sold_amount_raw
    from base_trades where rn <= 3
    ```

## reference examples
- custom DEX: `kuru_monad_base_trades.sql`
- uniswap v2 fork: `uniswap_v2_monad_base_trades.sql`
- uniswap v3 fork: `uniswap_v3_monad_base_trades.sql`

## notes
- for multi-version DEXs (v2 + v3), create separate models
- use version filter in seed test if sharing seed file
