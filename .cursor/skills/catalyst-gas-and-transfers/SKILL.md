---
name: catalyst-gas-and-transfers
description: Add gas fees and token transfer models for a new chain (hourly_spellbook + tokens)
disable-model-invocation: true
---
# catalyst gas and transfers

## overview
Sets up gas fees and token transfers for a new chain.

**When to use:** Use when adding gas and token transfer models for a chain that already has foundational metadata.

## parameters
- `<issue_id>`: linear issue id (e.g., CUR2-547)
- `<chain>`: chain name (e.g., monad)

## usage
```
/catalyst-gas-and-transfers CUR2-547 monad
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
2. **Create branch:** name `<issue_id>-<chain>-gas-and-transfers`, create off `main`, checkout, warn if exists. Don't commit/push anything.

## additional prep
- Identify native `token_address`: use Dune MCP **query_sql** with query: `select * from dune.blockchains where name = '<chain>'` (substitute `<chain>`).

## steps
1. **add gas fees model**
   - check chain docs: L1 or L2/rollup?
   - query `<chain>.transactions` for sample gas fees setup
   - create `dbt_subprojects/hourly_spellbook/models/_sector/gas/fees/<chain>/gas_<chain>_fees.sql`
   - L1: use `evm_l1_gas_fees` macro
   - L2: use `op_stack_gas_fees` or `arbitrum_orbit_stack_gas_fees`
   - if non-applicable: write custom logic

2. **add gas schema**
   - create `dbt_subprojects/hourly_spellbook/models/_sector/gas/fees/<chain>/gas_<chain>_schema.yml`

3. **add gas seed data**
   - edit `dbt_subprojects/hourly_spellbook/seeds/_sector/gas/evm_gas_fees.csv`
   - use Dune MCP **run_query_by_id** with `query_id: 6162940`, `query_parameters: '{"chain":"<chain>"}'` (substitute `<chain>`) for test entries

4. **add to gas fees union**
   - edit `dbt_subprojects/hourly_spellbook/models/_sector/gas/fees/gas_fees.sql`
   - add `<chain>` to both chain lists

5. **create transfer models**
   - create `dbt_subprojects/tokens/models/transfers_and_balances/<chain>/` with:
     - `tokens_<chain>_base_transfers.sql`
     - `tokens_<chain>_transfers.sql`
     - `tokens_<chain>_net_transfers_daily.sql`
     - `tokens_<chain>_net_transfers_daily_asset.sql`
       - set `native_contract_address = var('ETH_ERC20_ADDRESS')` or chain-specific
     - `tokens_<chain>_transfers_from_traces.sql`
     - `tokens_<chain>_transfers_from_traces_base.sql`
     - `tokens_<chain>_transfers_from_traces_base_wrapper_deposits.sql`

6. **add transfers schema**
   - create `dbt_subprojects/tokens/models/transfers_and_balances/<chain>/_schema.yml`
   - define all 7 models with tests and column descriptions

7. **add to transfers unions**
   - `dbt_subprojects/tokens/models/transfers_and_balances/tokens_transfers.sql`
   - `dbt_subprojects/tokens/models/transfers_and_balances/tokens_net_transfers_daily.sql`
   - `dbt_subprojects/tokens/models/transfers_and_balances/tokens_net_transfers_daily_asset.sql`

8. **add to transfers macro**
   - `dbt_subprojects/tokens/macros/transfers_from_traces/transfers_from_traces_exposed_blockchains_macro.sql`

9. **final checks**
   - From repo root: run `pipenv shell`, then run `dbt compile` in `dbt_subprojects/tokens` and in `dbt_subprojects/hourly_spellbook`. Fix any errors.
