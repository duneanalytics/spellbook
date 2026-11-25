---
name: dune-spellbook-runbook
description: Operational reference for the duneanalytics/spellbook dbt pipeline — sub-project topology, CI/CD internals, dataset lineage for dex.trades / tokens.transfers / prices, materialization strategies, and a failure-to-debug map. Use to orient in the repo or debug a pipeline failure, not to author a new model (see spellbook-build-model for that).
---

# Spellbook pipeline runbook

Operational reference for the public `duneanalytics/spellbook` dbt repo: 5 sub-projects on Trino/DuneSQL building core blockchain abstractions (dex.trades, tokens.transfers, prices.*, gas fees) across 51+ EVM chains plus Solana.

This is the ops/debug/orientation companion to `spellbook-build-model` (which covers authoring DEX platforms, chains, macros, and seeds). Reach here when you need the topology map, how the data flows, or what a failure means.

## Sub-projects

| Project | Path | Freshness | Key datasets |
|---|---|---|---|
| daily_spellbook | `dbt_subprojects/daily_spellbook/` | Daily | Sector spells, labels, balances, addresses |
| hourly_spellbook | `dbt_subprojects/hourly_spellbook/` | Hourly | prices.usd_daily/hourly/latest, gas fees |
| dex | `dbt_subprojects/dex/` | Hourly | dex.trades (51 EVM chains, ~57 platforms/chain) |
| tokens | `dbt_subprojects/tokens/` | Hourly | tokens.transfers (60 chains), tokens.erc20 |
| solana | `dbt_subprojects/solana/` | 6 hours | Solana-specific models |

## Key config files

- `dbt_subprojects/<project>/dbt_project.yml` — materialization defaults, post-hooks, vars
- `sources/` (170+ YAML files) — raw table definitions
- `dbt_macros/dune/incremental_predicate.sql` — time filtering (default 3-day window)
- `dbt_macros/dune/incremental.sql` — custom merge with `merge_skip_unchanged`
- `dbt_macros/dune/config_trino_properties.sql` — `expose_spells()` / `hide_spells()`
- `dbt_macros/dune/mark_as_spell.sql` — spell registration + vacuum
- `.github/workflows/dbt_run.yml` — reusable CI pipeline

## CI/CD pipeline

### CI (on PRs)

- **Trigger**: PR touching `dbt_subprojects/<project>/**`
- **Runner**: self-hosted `spellbook-trino-ci`, 90-minute timeout
- **Strategy**: `dbt slim ci` — S3-cached manifest, builds only modified + downstream
- **State selection**: `state:modified.body state:modified.macros` against the cached manifest. Config-only changes don't trigger CI — add a stamp comment to force it.
- **3-stage pipeline**:
  1. `dbt seed --select state:modified+` then `dbt run --select state:modified+` (full refresh)
  2. `dbt test` on all defined tests
  3. If incremental models are present: re-run without `--full-refresh`, then re-test
- **Test tables**: `hive.test_schema.git_dunesql_<commit_hash>_<table>` (~24hr TTL). Query these on Dune to inspect CI output.
- **Pre-commit hooks** (on push): `dbt-compile`, `check-script-ref-and-source`, `check-model-has-description`, `check-model-has-properties-file`, `forbid-crlf`.

### Deploy (on main merge)

- A deploy-trigger workflow dispatches to the deploy repo.
- dbt Cloud runs scheduled prod builds (hourly/daily per sub-project).

## Materialization strategies

| Type | When | Config |
|---|---|---|
| View | Default, lightweight transforms | `materialized='view'` |
| Table | Medium datasets, dimension tables | `materialized='table'`, delta format |
| Incremental | Large fact tables, event streams | `incremental_strategy='merge'`, `file_format='delta'`, `unique_key=[...]` |

**Partition strategies**:

- `block_month` — most common (dex.trades, tokens.transfers)
- `[blockchain, project, block_month]` — cross-chain sector spells
- Always include partition columns in `unique_key` for merge pruning.

## Dataset lineage: dex.trades

```
decoded contract events (Pair_evt_Swap, Pool_evt_Swap, ...)
  → dex_<chain>_<platform>_base_trades.sql (~57 platforms)
    → dex_<chain>_base_trades.sql (unions all platforms, validates non-negative amounts)
      → dex_<chain>_trades.sql (enrich_dex_trades macro: join tokens.erc20 + prices → USD)
        → dex_trades.sql (UNION ALL across 51 chains)
```

**Top model**: `dbt_subprojects/dex/models/trades/dex_trades.sql`

- Partition: `[block_month, blockchain, project]`
- Unique key: `[blockchain, project, version, tx_hash, evt_index, block_month]`
- Special case: Balancer V3 uses `enrich_balancer_v3_dex_trades` for ERC4626 share tokens.

## Dataset lineage: tokens.transfers

```
raw transactions/logs/traces (delta_prod)
  → tokens_<chain>_base_transfers.sql (transfers_base macro: ERC20 from logs)
  → tokens_<chain>_transfers_from_traces_base.sql (native from traces)
  → tokens_<chain>_transfers_from_traces_base_wrapper_deposits.sql (wrapped tokens)
  → tokens_<chain>_transfers_from_traces.sql (transfers_from_traces_macro)
  → tokens_<chain>_transfers.sql (transfers_enrich macro: join metadata + prices)
  → tokens_<chain>_net_transfers_daily.sql (daily aggregates)
  → tokens_<chain>_net_transfers_daily_asset.sql (per-asset daily)
  → tokens_transfers.sql (UNION ALL across 60 chains)
```

**7 model variants per chain.** The native token address varies per chain and must be researched — there's no universal registry.

## Dataset lineage: prices

- `prices.usd` (source) — minute-level from CoinPaprika + on-chain feeds
- `prices_usd_daily.sql` — OHLC aggregation to daily, excludes the current day
- `prices_usd_hourly.sql` — same, hourly granularity
- `prices_usd_latest.sql` — VIEW, full outer join of 2-day-new + historical-latest
- FX rates: generated in the sqlmesh sub-project from a daily FX API pull at 1am UTC.

## Failure-to-debug map

| Failure | Cause | Fix |
|---|---|---|
| CI runs too many models | Stale S3 manifest | Wait for the post-merge `commit_manifest` job |
| Merge produces duplicates | NULL in unique-key columns | Add `coalesce()` or a surrogate key |
| Seed test not found | Seed not in prod and not in PR | Include the seed file in the PR |
| Token prices missing | Backend backfill delay | Wait several days after adding to `prices_tokens` |
| `no_schema` CI failure | Model didn't set `schema` explicitly | Set a custom `schema` in config |

**Debug steps**:

1. Check the GH Actions run log for the failing stage.
2. Query the CI test table `hive.test_schema.git_dunesql_<commit_hash>_<table>` on Dune (~24hr window) to inspect output.
3. Compare CI output against the model's seed CSV.
4. Inspect `target/compiled/<project>/<model>.sql` for the rendered SQL.

## Trino / DuneSQL gotchas

- **Dispatch override**: `trino_utils` overrides `dbt_utils` via dispatch config. If a `dbt_utils` function behaves unexpectedly, check `trino_utils`.
- **Post-hook order**: writer scaling → disable task scale → optimize → mark_as_spell. The last two only run in prod.
- **Partition pruning**: filter on `block_date`, not `block_time`. Never apply functions to partition columns in `WHERE`. Include partition columns in `JOIN ON` clauses.
- **View security**: models default to `invoker` security — views check the caller's permissions, not the creator's.

## Related skills

- `spellbook-build-model` — authoring DEXes, chains, macros, and seeds (the how-to companion to this reference)
- `model-regression-testing` — pre-merge data-correctness gate once CI is green
- `debugging-ci` — when CI itself is red
