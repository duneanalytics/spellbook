---
name: spellbook-build-model
description: Build spellbook models -- DEX platforms, chain onboarding, macros, seeds. Use when adding a DEX, onboarding a chain, writing macros, or creating seeds. Includes macro internals, silent failure guards, and undocumented patterns.
---

# Build Spellbook Models

Use this skill when adding a new DEX, onboarding a chain, writing macros, or creating seeds. Each task includes the steps AND the tribal knowledge needed to get it right.

---

## Task A: Add a New DEX Platform

### A1. Identify the DEX type

```bash
# Uniswap V2 forks (most common)
grep -rl "uniswap_compatible_v2_trades" dbt_subprojects/dex/models/trades/
# Uniswap V3 forks
grep -rl "uniswap_compatible_v3_trades" dbt_subprojects/dex/models/trades/
# Custom (Curve, Balancer, etc.) — copy the closest existing one
ls dbt_subprojects/dex/models/trades/<chain>/platforms/
```

**Decision tree for macro vs inline SQL:**
1. Uniswap fork with standard Swap/PairCreated events? Use the macro.
2. Same custom logic across 3+ chains? Write a new macro in `macros/models/_project/`.
3. Complex but single-chain? Inline SQL is fine.
4. Inline file growing past ~200 lines? Extract a macro.

### A2. Create the base trades model

**Path**: `dbt_subprojects/dex/models/trades/<chain>/platforms/<project>_v<version>_<chain>_base_trades.sql`

For Uniswap V2 fork:
```sql
{{
    config(
        schema = '<project>_v<version>_<chain>',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{{
    uniswap_compatible_v2_trades(
        blockchain = '<chain>',
        project = '<project>',
        version = '<version>',
        Pair_evt_Swap = source('<project>_<chain>', 'Pair_evt_Swap'),
        Factory_evt_PairCreated = source('<project>_<chain>', 'Factory_evt_PairCreated')
    )
}}
```

**Macro internals you need to know:**

`uniswap_compatible_v2_trades()` params:
```
blockchain, project, version          — required, strings
Pair_evt_Swap, Factory_evt_PairCreated — required, source() references (not strings)
pair_column_name = 'pair'             — optional, some forks rename this column
```

Amount direction logic: if `amount0Out == 0`, trader bought token1. If `amount0In == 0 OR amount1Out == 0`, trader sold token1. This matters when debugging mismatched seed values.

`uniswap_compatible_v3_trades()` params:
```
blockchain, project, version          — required
Pair_evt_Swap, Factory_evt_PoolCreated — required, source() references
taker_column_name = 'recipient'       — varies across forks, check decoded schema
maker_column_name = null              — most forks don't have this
optional_columns = ['f.fee']          — pass [] if fork has no fee column
pair_column_name = 'pool'             — some forks rename this
```

V3 uses signed int256: `amount0 < 0` means buying token0. Both sides use absolute values.

**Guards:**
- `unique_key` columns must never contain NULLs. Trino MERGE treats `NULL != NULL`, creating silent duplicates on every run. Use `COALESCE()` or `dbt_utils.generate_surrogate_key()`.
- `incremental_predicates` must reference `DBT_INTERNAL_DEST.<column>` (the destination table alias used internally by dbt).
- Every model MUST set `schema` explicitly. Default is `+schema: no_schema` which fails CI via `scripts/check_schema.sh`.

### A3. Define sources

**Path**: `sources/_sector/dex/trades/<chain>/_sources.yml`

```yaml
sources:
  - name: <project>_<chain>
    tables:
      - name: Pair_evt_Swap
        loaded_at_field: evt_block_time
      - name: Factory_evt_PairCreated
        loaded_at_field: evt_block_time
```

### A4. Add to chain union

**Path**: `dbt_subprojects/dex/models/trades/<chain>/dex_<chain>_base_trades.sql`

Add the new platform model to the UNION ALL.

**CRITICAL: This is the #1 silent failure.** If you skip this step, the model builds fine but data never reaches `dex.trades`. Always verify your ref appears in the union file.

### A5. Create seed file

**Path**: `dbt_subprojects/dex/seeds/trades/<project>_<chain>_base_trades_seed.csv`

Get 3-5 recent trades from production:
```sql
SELECT blockchain, project, version,
    cast(block_date as varchar) as block_date,
    tx_hash, evt_index, token_bought_address, token_sold_address,
    block_number, token_bought_amount_raw, token_sold_amount_raw
FROM dex.trades
WHERE blockchain = '<chain>' AND project = '<project>'
  AND block_date >= CURRENT_DATE - INTERVAL '7' DAY
ORDER BY block_time DESC LIMIT 5
```

Add seed definition to `dbt_subprojects/dex/seeds/trades/_schema.yml`. Column types must be Trino types: `varbinary`, `uint256`, `double`, `varchar`, `timestamp`, `int`.

**Guard:** Use recent transactions (last 7 days). Decoded source tables can be reprocessed, dropping old txs. Stale seeds fail with `matched records count = 0`.

### A6. Register the DEX

Add to `dbt_subprojects/dex/models/dex_info.sql`:
```sql
, ('<chain>', '<project>', '<version>', '<display_name>')
```

### A7. Compile and test

```bash
cd dbt_subprojects/dex
dbt deps  # first time only
dbt compile --select <project>_v<version>_<chain>_base_trades
# Check target/compiled/ for valid SQL
```

**Fast iteration:** Set `dev_dates: true` in `dbt_subprojects/dex/dbt_project.yml` to limit scans to 3 days. ALWAYS revert before merge — leaving it on silently limits production to 3 days of data forever.

### A8. Verify checklist before pushing

- [ ] Model added to `dex_<chain>_base_trades.sql` union
- [ ] Sources defined in `sources/_sector/dex/trades/<chain>/_sources.yml`
- [ ] Seed CSV uses recent txs (last 7 days)
- [ ] Seed registered in `_schema.yml` with correct column types
- [ ] DEX registered in `dex_info.sql`
- [ ] `schema` set explicitly in config (not `no_schema`)
- [ ] No NULLs possible in `unique_key` columns
- [ ] `incremental_predicate` on all time-series source joins
- [ ] `dev_dates` is `false` in `dbt_project.yml`
- [ ] `dbt compile` succeeds

---

## Task B: Add a New Chain (3-PR Process)

### PR 1: Foundational Metadata (tokens + daily_spellbook)

| Step | File | Action |
|------|------|--------|
| 1 | `dbt_subprojects/daily_spellbook/models/evms/evms_info.sql` | Add chain row |
| 2 | `dbt_subprojects/tokens/models/.../prices_native_tokens.sql` | Add native token |
| 3 | `dbt_subprojects/tokens/models/.../prices_<chain>_tokens.sql` | Create: top 5 + stables |
| 4 | `dbt_subprojects/tokens/models/.../prices_tokens.sql` | Add to union |
| 5 | `sources/_base_sources/evm/<chain>_base_sources.yml` | Define raw tables |
| 6 | `dbt_subprojects/daily_spellbook/models/evms/evms_blockchains_list.sql` | Add chain |

**Script shortcut:** `scripts/generate_evm_sources.py` generates `_base_sources.yml` from a Jinja template. Add the chain to the hardcoded list in the script, then run:
```bash
python scripts/generate_evm_sources.py
```

**Guard:** Research the correct native token address for this chain. It varies and there's no universal registry.

### PR 2: Transfers + Fees (after PR1 merge)

| Step | File | Action |
|------|------|--------|
| 1 | `dbt_subprojects/hourly_spellbook/models/.../gas_<chain>_fees.sql` | Create using L1/OP Stack/Arbitrum Orbit macro |
| 2 | `dbt_subprojects/tokens/models/transfers_and_balances/<chain>/` | Create 7 models (use macros) |
| 3 | Union views | Add chain to `tokens_transfers.sql`, `tokens_net_transfers_daily.sql` |

**Script shortcut:** `scripts/replicate_dbt_all_evms.py` replicates a model template across all EVM chains. Useful for the 7 transfer models.

### PR 3: DEX Integration (after PR1 merge)

Follow Task A above for each DEX on the chain, plus create the chain-level union and enriched models.

**Script shortcut:** `scripts/dex-sep-gen.py` generates per-project DEX spell view files for multi-chain DEXes.

---

## Task C: Write a New Macro

### When to write a macro
- Same SQL pattern across 3+ chains or platforms
- Parameterizable by `blockchain`, `project`, `version`

### Convention
- File: `dbt_subprojects/<project>/macros/models/_project/<macro_name>.sql` (project-specific) or `_sector/` (sector-level)
- Accept `blockchain` as first arg
- Use `source()` and `ref()` inside, never hardcoded table names
- Always apply `incremental_predicate` to time-series source joins inside the macro

### Pattern
```sql
{% macro my_macro(blockchain) %}
SELECT
    '{{ blockchain }}' as blockchain,
    ...
FROM {{ source('<namespace>_' ~ blockchain, '<table>') }}
{% if is_incremental() %}
WHERE {{ incremental_predicate('block_time') }}
{% endif %}
{% endmacro %}
```

### Wrapper model
```sql
{{ config(
    schema = '<namespace>_<chain>',
    alias = '<model_name>',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['tx_hash', 'evt_index'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
)}}
{{ my_macro('<chain>') }}
```

---

## Task D: Expose a Model to Dune Data Explorer

```sql
{{ config(
    ...
    post_hook = '{{ expose_spells(
        blockchains = \'["ethereum", "arbitrum"]\',
        spell_type = "project",
        spell_name = "<project_name>",
        contributors = \'["<dune_handle>"]\')
    }}'
)}}
```

`blockchains` and `contributors` MUST be valid JSON strings (arrays). The macro validates with `tojson(fromjson(...))` and raises a compiler error if invalid. Only runs in prod target.

To deprecate a model, add `deprecated_at` to its config:
```sql
{{ config(..., deprecated_at = '2025-06-01') }}
```

---

## Macro Quick Reference

### `add_amount_usd(trades_cte)`
Takes a **CTE name** (not a ref). Required columns: `blockchain, token_bought_address, token_sold_address, token_bought_amount, token_sold_amount, block_time`. USD fallback: trusted bought token price, then trusted sold, then bought, then sold. Prices joined at minute granularity.

### `add_tx_columns(model_cte, blockchain, columns=[])`
Takes a **CTE name** (not a ref). Joins transactions on `block_date + block_number + tx_hash` — all three must exist in your CTE or the join silently returns zero rows. Output columns are prefixed with `tx_` (e.g., `columns=['nonce']` becomes `tx_nonce`).

### `incremental_predicate(column)`
Expands to: `column >= date_trunc('day', now() - interval '3' day)`. Override via env vars `DBT_ENV_INCREMENTAL_TIME` and `DBT_ENV_INCREMENTAL_TIME_UNIT`.

### `incremental_days_forward_predicate(column, base_time, days_forward)`
Windowed range for chunking large scans: `column BETWEEN (base_time - lookback) AND (base_time + days_forward)`.

---

## Silent Failures Checklist

Run through this before every push:

1. **Union registration** — Is the model added to `dex_<chain>_base_trades.sql`? Data silently missing if not.
2. **NULL unique keys** — Any column in `unique_key` that can be NULL? Silent duplicates on every run.
3. **Incremental predicates** — Applied to ALL time-series source joins? Missing ones cause full table scans in prod (CI won't catch this since it does full refresh).
4. **dev_dates** — Is it `false` in `dbt_project.yml`? If `true`, prod processes only 3 days forever.
5. **Schema override** — Does the model set `schema` explicitly? Default `no_schema` fails CI.
6. **Seed freshness** — Are seed txs from the last 7 days? Older txs may vanish after source reprocessing.
7. **add_tx_columns join cols** — Does the CTE have `block_date`, `block_number`, AND `tx_hash`?

---

## Utility Scripts

Undocumented scripts in `scripts/` that save significant manual work:

| Script | Purpose | Usage |
|--------|---------|-------|
| `generate_evm_sources.py` | Generate `_base_sources.yml` for all 43 EVM chains | `python scripts/generate_evm_sources.py` (from repo root) |
| `dex-sep-gen.py` | Generate per-project DEX spell view files | For multi-chain DEX onboarding |
| `replicate_dbt_all_evms.py` | Replicate a model template across all EVM chains | For transfer/fee models |
| `generate_platform_views.py` | Generate platform-level aggregation views | For DEX project views |
| `token_checker.py` | Validate token metadata against CoinPaprika API | Run against token seed files |
| `check_schema.sh` | Verify all models have custom schema | `cd dbt_subprojects/<project> && bash ../../scripts/check_schema.sh` |
| `list_modified_downstream.sh` | List blast radius of your changes | `bash scripts/list_modified_downstream.sh` |

---

## Environment Variables

| Variable | Default | Override for |
|----------|---------|-------------|
| `DBT_ENV_INCREMENTAL_TIME` | `3` | Lookback window size. Set to `1` for fast local runs. |
| `DBT_ENV_INCREMENTAL_TIME_UNIT` | `day` | Lookback unit. |
| `DUNE_API_KEY` | none | Required for `scripts/dune_query.py`. |

```bash
# Fast local run: 1-day lookback
DBT_ENV_INCREMENTAL_TIME=1 dbt run --select my_model
```

**dbt_project.yml vars**: `dev_dates` (false, NEVER merge as true), `freshness` (hourly/daily per subproject), `ETH_ERC20_ADDRESS` (0x000...000, placeholder for native ETH in null-token checks).

---

## CI Internals

- **State selection**: CI runs `state:modified.body state:modified.macros` against S3-cached manifest. Config-only changes don't trigger CI — add a stamp comment to force it.
- **Pipeline**: seed → run (full refresh) → test → if incremental: run again (incremental) → test again.
- **Test tables**: `hive.test_schema.git_dunesql_<commit_hash>_<table>` (~24hr TTL). Query on Dune to inspect CI output.
- **Pre-commit hooks** (on push): `dbt-compile`, `check-script-ref-and-source`, `check-model-has-description`, `check-model-has-properties-file`, `forbid-crlf`.

---

## Trino/DuneSQL Gotchas

- **Dispatch override**: `trino_utils` overrides `dbt_utils` via dispatch config. If a dbt_utils function behaves unexpectedly, check trino_utils.
- **Post-hook order**: writer scaling → disable task scale → optimize → mark_as_spell. Hooks 3-4 only run in prod.
- **Partition pruning**: Filter on `block_date` not `block_time`. Never apply functions to partition columns in WHERE. Include partition columns in JOIN ON clauses.
- **View security**: All models default to `invoker` security — views check caller permissions, not creator's.
- **writer_min_size**: Default `'500MB'` in config controls write parallelization. Override in model config for very large tables.
