---
name: catalyst-foundational-metadata
description: Set up new chain foundational metadata (evms_info, prices, base sources, evms_blockchains_list)
disable-model-invocation: true
---
# catalyst foundational metadata

## overview
Sets up foundational metadata for a new chain.

**When to use:** Use when adding a new chain's foundational metadata (evms_info, native/prices tokens, base sources, evms_blockchains_list).

## parameters
- `<issue_id>`: linear issue id (e.g., CUR2-554)
- `<chain>`: chain name (e.g., kaia, monad)

## usage
```
/catalyst-foundational-metadata CUR2-554 xlayer
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
2. **Create branch:** name `<issue_id>-<chain>-foundational-metadata`, create off `main`, checkout, warn if exists. Don't commit/push anything.

## steps
1. **add EVM chain info**
   - edit `dbt_subprojects/daily_spellbook/models/evms/evms_info.sql`
   - add `<chain>` to `expose_spells` list
   - add VALUES row: `(chain_id, '<chain>', 'Name', 'Layer 1/2', ...)`
   - use prep vars: chain_id, name, first_block_time, token_address
   - find: explorer, wrapped_native_token_address

2. **add native token**
   - find id on: https://api.coinpaprika.com/v1/coins
   - add to `dbt_subprojects/tokens/models/prices/prices_native_tokens.sql`

3. **create prices tokens model**
   - create `dbt_subprojects/tokens/models/prices/<chain>/prices_<chain>_tokens.sql`
   - check chain docs for token addresses & symbols
   - if not found: use Dune MCP **run_query_by_id** with `query_id: 6293737`, `query_parameters: '{"chain":"<chain>"}'` (substitute `<chain>`)
   - identify key tokens (top 5 transferred, stables, WETH)
   - find ids on coinpaprika, add to VALUES

4. **create schema file**
   - create `dbt_subprojects/tokens/models/prices/<chain>/_schema.yml`

5. **add to prices union**
   - edit `dbt_subprojects/tokens/models/prices/prices_tokens.sql`
   - add `ref('prices_<chain>_tokens')` to `fungible_prices_models`

6. **define raw data sources**
   - create `sources/_base_sources/evm/<chain>_base_sources.yml`
   - create `sources/_base_sources/evm/<chain>_docs_block.md`
   - use existing patterns or `scripts/generate_evm_*.py`

7. **integrate into aggregate EVM models**
   - add `<chain>` to `dbt_subprojects/daily_spellbook/macros/helpers/evms_blockchains_list.sql`

8. **final checks**
   - From repo root: run `pipenv shell`, then run `dbt compile` in `dbt_subprojects/tokens` and in `dbt_subprojects/daily_spellbook`. Fix any errors.
