# catalyst foundational metadata

## overview
sets up foundational metadata for a new chain

## parameters
- `<issue_id>`: linear issue id (e.g., CUR2-554)
- `<chain>`: chain name (e.g., kaia, monad)

## usage
```
/catalyst/foundational-metadata CUR2-554 xlayer
```

## prerequisites
1. run `_shared.md#git-workflow` (task_suffix: `foundational-metadata`)
2. run `_shared.md#prep-vars`

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
  - if not found: run `.cursor/scripts/dune_query.py --query-id 6293737 --param chain=<chain>`
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
  - run `_shared.md#final-checks` (tokens, daily_spellbook)
