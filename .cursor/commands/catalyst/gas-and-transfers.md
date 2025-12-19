# catalyst gas and transfers

## overview
sets up gas fees and token transfers for a new chain

## parameters
- `<issue_id>`: linear issue id (e.g., CUR2-547)
- `<chain>`: chain name (e.g., monad)

## usage
```
/catalyst/gas-and-transfers CUR2-547 monad
```

## prerequisites
1. run `_shared.md#git-workflow` (task_suffix: `gas-and-transfers`)
2. run `_shared.md#prep-vars`

additional prep:
- identify native `token_address`: `.cursor/scripts/dune_query.py --query "select * from dune.blockchains where name = '<chain>'"`

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
  - run query_id=6162940 + `<chain>` param for test entries

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
  - run `_shared.md#final-checks` (tokens, hourly_spellbook)
