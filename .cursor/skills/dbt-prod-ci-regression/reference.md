# Reference: prod vs CI regression query pattern

## CI table naming (Spellbook / Dune CI)

Tables are written under **`test_schema`** with a name like:

`git_dunesql_<commit_hash>_<schema>_<alias>`

Spellbook CI sets `GIT_SHA` to the **first 7 characters** of `${{ github.sha }}` after `tr - _` (`dbt_run.yml`). For **`pull_request`** workflows, `github.sha` is the **PR merge commit** (test merge of head + base), **not** the latest commit on your branch—so the hash differs from `4ed0409…` (head) vs `8d61409…` (merge). Use `git fetch origin pull/<N>/merge` and `git rev-parse FETCH_HEAD | tr - _ | cut -c1-7`, or read the hash from **GitHub Actions**.

The suffix after the hash is **`{custom_schema}_{alias}`** from the model’s dbt **`schema`** and **`alias`** config (same pieces that form prod `schema.table`). Example: `schema = 'tokens'` and `alias = 'transfers'` → **`tokens_transfers`** → `test_schema.git_dunesql_<GIT_HASH>_tokens_transfers`. It is **not** the `.sql` filename alone—derive from config or copy the exact identifier from **`dbt run initial model(s)`** (README: `test_schema.git_dunesql_{{commit_hash}}_{{table_name}}`).

Tables are usually available for **~24 hours** after the CI run.

After you fill in hash, **run phase 1 on Dune**, substitute bounds, then run parity / grain / rollup via **Dune MCP** (`createDuneQuery` → `executeQueryById` → `getExecutionResults`); read each tool’s schema under `mcps/user-dune/tools/` before calling. Spellbook fallback: repo `python scripts/dune_query.py` when available. Details in [SKILL.md](SKILL.md).

## Dynamic workflow (do not hardcode example dates)

1. **Phase 1 — execute** a bounds query on the **CI** table only (below). Record `min_block_date` and `min_block_number` from the result row.
2. **Phase 2+ — substitute** literals into all downstream SQL: `date '<MIN_BLOCK_DATE>'` and `>= <MIN_BLOCK_NUMBER>` (omit `block_number` if not meaningful for the spell).
3. **Never** reuse historical dates from docs as if they were current; they illustrate shape only.

Replace `<GIT_HASH>` using `git fetch origin pull/<N>/merge` and `git rev-parse FETCH_HEAD | tr - _ | cut -c1-7` for Spellbook **`pull_request`** CI.

## Phase 1: CI bounds (run first, every time)

```sql
select
	min(block_date) as min_block_date
	, min(block_number) as min_block_number
from
	test_schema.git_dunesql_<GIT_HASH>_tokens_transfers
where
	block_date != current_date
```

## Shared `where` fragment (paste into phase 2+)

Use the **same** predicates on CI and prod CTEs (tokens example; add/remove columns per schema):

```text
block_date != current_date
	and block_date >= date '<MIN_BLOCK_DATE>'
	and block_number >= <MIN_BLOCK_NUMBER>
```

**Metrics:** default pair is **`count(1)`** and **`sum(amount_usd)`** when `amount_usd` exists—pick columns from **`_schema.yml`**, model SQL, or Dune column list for other spells.

## Example: grain diff query (`tokens.transfers`)

Grain is **`blockchain`** here only; other spells use **`block_month`**, **`block_date`**, **`project`**, or composites—mirror in **`group by`** and **`join`**.

```sql
with ci as (
	select
		blockchain
		, count(1) as total_rows
		, sum(amount_usd) as total_usd
	from
		test_schema.git_dunesql_<GIT_HASH>_tokens_transfers
	where
		block_date != current_date
		and block_date >= date '<MIN_BLOCK_DATE>'
		and block_number >= <MIN_BLOCK_NUMBER>
	group by
		blockchain
)
, prod as (
	select
		blockchain
		, count(1) as total_rows
		, sum(amount_usd) as total_usd
	from
		tokens.transfers
	where
		block_date != current_date
		and block_date >= date '<MIN_BLOCK_DATE>'
		and block_number >= <MIN_BLOCK_NUMBER>
	group by
		blockchain
)
select
	prod.blockchain
	, prod.total_rows as prod_rows
	, ci.total_rows as ci_rows
	, abs(prod.total_rows - ci.total_rows) as diff_rows
	, prod.total_usd as prod_usd
	, ci.total_usd as ci_usd
	, abs(prod.total_usd - ci.total_usd) as diff_usd
from
	prod
inner join ci
	on prod.blockchain = ci.blockchain
where
	(
		abs(prod.total_rows - ci.total_rows) > 0
		or abs(prod.total_usd - ci.total_usd) > 5
	)
order by
	1
```

## Rollup: raw rows + raw USD + grain counts + diff grains

One result row: **CI/prod raw counts**, **CI/prod raw `sum(amount_usd)`**, diffs, **inner-join grain row count**, **sums of grain-level USD** over the join (sanity vs raw), **diff grain count**.

```sql
with shared_filters_ci as (
	select
		count(1) as ci_raw_rows
		, sum(amount_usd) as ci_raw_usd
	from
		test_schema.git_dunesql_<GIT_HASH>_tokens_transfers
	where
		block_date != current_date
		and block_date >= date '<MIN_BLOCK_DATE>'
		and block_number >= <MIN_BLOCK_NUMBER>
)
, shared_filters_prod as (
	select
		count(1) as prod_raw_rows
		, sum(amount_usd) as prod_raw_usd
	from
		tokens.transfers
	where
		block_date != current_date
		and block_date >= date '<MIN_BLOCK_DATE>'
		and block_number >= <MIN_BLOCK_NUMBER>
)
, ci as (
	select
		blockchain
		, count(1) as total_rows
		, sum(amount_usd) as total_usd
	from
		test_schema.git_dunesql_<GIT_HASH>_tokens_transfers
	where
		block_date != current_date
		and block_date >= date '<MIN_BLOCK_DATE>'
		and block_number >= <MIN_BLOCK_NUMBER>
	group by
		blockchain
)
, prod as (
	select
		blockchain
		, count(1) as total_rows
		, sum(amount_usd) as total_usd
	from
		tokens.transfers
	where
		block_date != current_date
		and block_date >= date '<MIN_BLOCK_DATE>'
		and block_number >= <MIN_BLOCK_NUMBER>
	group by
		blockchain
)
, joined as (
	select
		prod.blockchain
		, prod.total_rows as prod_rows
		, ci.total_rows as ci_rows
		, prod.total_usd as prod_usd
		, ci.total_usd as ci_usd
		, abs(prod.total_rows - ci.total_rows) as diff_rows
		, abs(prod.total_usd - ci.total_usd) as diff_usd
	from
		prod
	inner join ci
		on prod.blockchain = ci.blockchain
)
select
	sf_ci.ci_raw_rows
	, sf_prod.prod_raw_rows
	, sf_prod.prod_raw_rows - sf_ci.ci_raw_rows as raw_row_count_diff
	, sf_ci.ci_raw_usd
	, sf_prod.prod_raw_usd
	, abs(sf_prod.prod_raw_usd - sf_ci.ci_raw_usd) as raw_usd_abs_diff
	, (select count(1) from joined) as inner_join_grain_rows
	, (select sum(j.prod_usd) from joined as j) as inner_join_sum_prod_usd
	, (select sum(j.ci_usd) from joined as j) as inner_join_sum_ci_usd
	, abs(
		(select sum(j.prod_usd) from joined as j)
		- (select sum(j.ci_usd) from joined as j)
	) as inner_join_usd_abs_diff
	, (
		select
			count(1)
		from
			joined as j2
		where
			j2.diff_rows > 0
			or j2.diff_usd > 5
	) as diff_grain_rows
from
	shared_filters_ci as sf_ci
cross join shared_filters_prod as sf_prod
```

- **`raw_row_count_diff` / `raw_usd_abs_diff`** — global parity on the shared window.
- **`inner_join_grain_rows`** — intersecting grain keys (e.g. 54 for `blockchain`-only when aligned).
- **`inner_join_*_usd`** — should match **`ci_raw_usd` / `prod_raw_usd`** when every row rolls up to exactly one grain and the inner join is complete.
- **`diff_grain_rows = 0`** — no grain over row / USD tolerance.

## Row-count parity only (quick check after phase 1)

```sql
-- same where fragment on both
select count(1) from tokens.transfers
where block_date != current_date and block_date >= date '<MIN_BLOCK_DATE>' and block_number >= <MIN_BLOCK_NUMBER>;

select count(1) from test_schema.git_dunesql_<GIT_HASH>_tokens_transfers
where block_date != current_date and block_date >= date '<MIN_BLOCK_DATE>' and block_number >= <MIN_BLOCK_NUMBER>;
```

## Full outer join (optional)

Inspect missing grain keys before applying the diff-only `where`:

```sql
select coalesce(prod.blockchain, ci.blockchain) as blockchain, ...
from prod
full outer join ci on prod.blockchain = ci.blockchain
```
