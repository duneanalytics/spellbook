---
name: dbt-prod-ci-regression
description: >-
  Designs and runs DuneSQL regression queries comparing CI test_schema tables to production spells after
  pipeline-only dbt changes; after SQL is tailored to the feature branch and CI table name, uses Dune MCP
  to execute queries and validate parity. Use for prod vs CI regression, lineage parity checks, or row/metric
  validation when data should not drift; invoke manually per branch—not on every edit.
---

# dbt prod vs CI regression queries

## Intent

- **Pipeline / logic change only**: compiled SQL or merge behavior changed; **prod and CI should match** on a chosen time/block window.
- **Manual workflow**: user or agent runs this when needed; **do not assume** every model edit triggers regression.
- **Adapt per lineage**: reuse **structure** (explore → align filters → count check → join → diff filter); swap **tables**, **grain**, **metrics**, and **filters** from the models under test.

## Resolve CI table name

1. **From user**: commit SHA or direct `test_schema.git_dunesql_<hash>_<schema>_<alias>`.
2. **From git (Spellbook)**: Sub-project workflows use **`on: pull_request`** (e.g. `tokens.yml`). For those runs, `${{ github.sha }}` in `dbt_run.yml` is the **merge commit** GitHub builds for `refs/pull/<N>/merge`—**not** the PR branch head shown in the PR commits list. So **`origin/<branch>` tip (e.g. `4ed0409…`) will not match** the CI hash (e.g. `8d61409…`).  
   **Derive locally:** `git fetch origin pull/<PR_NUMBER>/merge` then `git rev-parse FETCH_HEAD | tr - _ | cut -c1-7`.  
   **Or** copy the hash from **Actions** (workflow run / logs). Workflows triggered by **`push`** (if any) would use the pushed commit—then `origin/<branch>` matches.
3. **Suffix**: **`{custom_schema}_{alias}`** from the model config (`tokens.transfers` → `tokens_transfers`). Confirm in **`dbt run initial model(s)`** when unsure.
4. **From GitHub Actions**: logs show the exact materialized name if discovery fails.

Spellbook note: see also [.cursor/skills/debug-ci/SKILL.md](../debug-ci/SKILL.md) for CI context. **Never** embed API keys in the skill.

## Run and validate (Dune MCP)

After SQL is built from **branch context** (PR merge SHA or correct `github.sha` source + `{schema}_{alias}`, filters):

1. **Read MCP tool schemas first** (required): under the workspace `mcps/user-dune/tools/` (or the enabled Dune server’s tool descriptors), open the JSON for each tool you call so arguments match the contract.
2. **`createDuneQuery`**: create a **temporary** query (`is_temp` defaults true) with a clear `name`, the full DuneSQL `query` text, and optional `description`. Capture the returned **`query_id`**.
3. **`executeQueryById`**: run with that **`query_id`** (and `performance` if needed). Capture **`execution_id`** from the response.
4. **`getExecutionResults`**: pass **`executionId`** (ULID), increase **`timeout`** for heavy scans, use **`limit`** for previews. Interpret **`state`**: `COMPLETED` → check `data.rows` and `resultMetadata.totalRowCount`; `FAILED` → use `errorMessage` / `errorMetadata` to fix SQL and repeat.

Run **exploratory** SQL (min dates, raw counts) and the **final diff** query through the same pipeline. If MCP is unavailable, fall back to **`python scripts/dune_query.py`** (repo root, `DUNE_API_KEY` in `.env`).

**Partitioning**: Dune tooling expects filters on partition columns (e.g. `block_date`) where applicable—keep regression windows as tight as the comparison allows.

## Investigate modified schema

1. Use **`git diff`**, **`dbt compile`**, **`_schema.yml`**, and model SQL to list **relevant columns** (partition keys, `block_date`, `block_time`, `block_number`, metrics like `amount_usd`, etc.). When in doubt, inspect the **Dune table schema** or compiled SQL for CI and prod.
2. **Default comparison metrics** (when columns exist): **`count(1)`** (rows) and **`sum(amount_usd)`** (or the spell’s primary USD column). That pair usually surfaces pipeline issues quickly. **Extend or swap** metrics from the schema (e.g. `sum(amount_raw)`, volume fields) per lineage—tokens-style spells are the template, not a universal rule.
3. Choose a **grain** (`group by` / **join keys**) that exists on **both** CI and prod. It is **not always `blockchain`**: common Spellbook grains include **`blockchain`**, **`block_month`**, **`block_date`**, **`project`**, or combinations (e.g. `blockchain` + `block_date`). Match what the spell actually keys on for the question you are answering.
4. **Coarse-first, then deeper**: use **high-level aggregates** (single chain, or `block_month` / `block_date` slices) to confirm totals and distributions **before** joining on a **row-level unique key** (`unique_key`, `tx_hash` + `evt_index`, etc.). Cheap grain checks catch most pipeline regressions; unique-key diffs are for pinpointing survivors.
5. If there is **no** USD column, use **`count(1)`** and other numeric columns that make sense from the schema.

## Query workflow (order matters)

**Dynamic filters (required):** Do **not** copy example `block_date` / `block_number` literals from docs or chat. **Always** run **phase 1** on Dune (or MCP), read the result set, then **substitute** the returned bounds into every later query. Examples in [reference.md](reference.md) use placeholders `<MIN_BLOCK_DATE>`, `<MIN_BLOCK_NUMBER>` for that reason.

1. **Phase 1 — bounds on CI only** (execute first, every run):  
   `select min(block_date) as min_block_date, min(block_number) as min_block_number from <ci_table> where block_date != current_date`  
   (Add extra predicates only if you need a narrower probe.) Use the result as **shared** lower bounds for **both** CI and prod in phases 2+.
2. **Shared `where` fragment** (same on all CTEs):  
   `block_date != current_date and block_date >= date '<MIN_BLOCK_DATE>' and block_number >= <MIN_BLOCK_NUMBER>` — adjust or drop `block_number` if the model is not chain-scoped that way.
3. **Parity check**: **`count(1)`** and **`sum(<metric>)`** on **prod** and **ci** with the **identical** `where`; row count and metric totals should match before trusting a grain **inner join**.
4. **Aggregate CTEs**: same `where`, same `group by` grain, same metrics (e.g. `count(1)`, `sum(amount_usd)`).
5. **Join**: **`inner join` on the full grain** (all `group by` columns). Use **`full outer join`** temporarily to inspect missing keys.
6. **Diff filter last**: `abs(diff_rows) > 0` or `abs(diff_usd) > <tolerance>` (e.g. **5** for float noise) only after spot-checking unfiltered join output.
7. **Validate**: execute via **Dune MCP**; **0 rows** in the diff-filtered grain query ⇒ pass at that tolerance.

## Analysis outputs (what to report)

Build the **shared `where`** from **phase 1** bounds (see workflow). Keep it on every CTE; join only on grain keys.

| Output | Meaning |
|--------|---------|
| **CI row count** | `count(1)` on CI with full shared `where`. |
| **Prod row count** | Same on prod. |
| **Row-count diff** | `prod - ci` (expect **0** before grain join). |
| **CI raw metric** | e.g. **`sum(amount_usd)`** on CI with the same `where` (spell-dependent column). |
| **Prod raw metric** | Same on prod. |
| **Raw metric abs diff** | e.g. **`abs(prod_usd - ci_usd)`** (use tolerance for floats). |
| **Inner-join grain count** | `count(1)` from **`prod_agg inner join ci_agg`** **without** diff filter. |
| **Inner-join metric sums** | `sum(prod grain total_usd)` vs `sum(ci grain total_usd)` over that join (should match raw totals when every row maps to one grain and keys align). |
| **Diff grain count** | `count(1)` from the join **with** diff filter on rows / USD. **0** ⇒ pass. |

Optional: **`count(distinct …)`** per grain column on each side vs inner-join count.

See [reference.md](reference.md) for **phase 1** SQL, **placeholder** filters, grain diff query, and a **rollup** that returns counts + USD in one row.

## Template (fill placeholders)

```sql
with ci as (
	select
		<grain_columns>
		, count(1) as total_rows
		, sum(<metric_column>) as total_metric
	from
		test_schema.git_dunesql_<GIT_HASH>_<schema>_<alias>
	where
		block_date != current_date
		and block_date >= date '<MIN_BLOCK_DATE>'
		and <optional_aligned_predicates_e_g_block_number>
	group by
		<grain_columns>
)
, prod as (
	select
		<grain_columns>
		, count(1) as total_rows
		, sum(<metric_column>) as total_metric
	from
		<prod_catalog>.<prod_schema>.<prod_alias>
	where
		block_date != current_date
		and block_date >= date '<MIN_BLOCK_DATE>'
		and <same_optional_predicates_as_ci>
	group by
		<grain_columns>
)
select
	<compare_columns>
from prod
inner join ci
	on <join_keys>
where
	(
		abs(prod.total_rows - ci.total_rows) > 0
		or abs(prod.total_metric - ci.total_metric) > <tolerance>
	)
order by
	1
```

## Cross-repo use

The same workflow applies in **any** dbt + Dune (or Trino) repo: adjust **prod** relation (`catalog.schema.table`), **CI schema/table prefix**, and **column names**. Copy this folder to **`~/.cursor/skills/dbt-prod-ci-regression/`** for a personal default.

## More examples

See [reference.md](reference.md) for a filled **tokens.transfers** example and exploratory snippets.
