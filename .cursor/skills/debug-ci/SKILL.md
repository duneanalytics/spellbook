# Debug CI Failures

Use this skill when a Spellbook PR's CI tests fail and you need to diagnose the issue.

## Step 1: Identify Which Sub-Project Failed

Each sub-project has its own CI workflow (`daily_spellbook.yml`, `dex.yml`, `hourly_spellbook.yml`, `nft.yml`, `solana.yml`, `tokens.yml`). All call the shared reusable workflow in `.github/workflows/dbt_run.yml`.

Check the PR's "Checks" tab to see which workflow(s) failed.

## Step 2: Identify Which Step Failed

The CI pipeline runs these steps in order:

1. **`dbt seed`** — builds seed CSVs modified in the PR
2. **`dbt run initial model(s)`** — full historical build of modified models
3. **`dbt test initial model(s)`** — runs tests on the initial build
4. **`dbt run incremental model(s)`** — incremental run to test incremental logic
5. **`dbt test incremental model(s)`** — tests again post-incremental run

Click into the failed workflow run in the "Actions" tab to expand each step's logs.

## Step 3: Diagnose Common Failures

### Compile / Seed Failures
- **Missing source or ref**: Check that all `source()` and `ref()` calls point to valid models. Run `dbt compile` locally to verify.
- **Missing seed metadata**: If error says "Metadata is not found for ___", the seed likely doesn't exist in prod. Force a change on the seed file and include it in the PR so CI rebuilds it.

### Model Run Failures
- **SQL syntax errors**: Check the compiled SQL in CI logs. Run `dbt compile` locally and test the output on Dune or via `dune_query.py`.
- **Timeout (90-minute limit)**: Simplify the model, add tighter date filters, or check for missing partition filters.
- **Schema not assigned**: Every model must have `schema` in its config block. The CI check-schemas script will fail otherwise.

### Test Failures
- **`unique_combination_of_columns` failing**: The model is producing duplicate rows for the unique key. Check your join logic, filters, and whether NULLs are present in key columns.
- **`not_null` failing**: A unique key column contains NULLs. Add `coalesce()` or filter out NULL rows.
- **Seed test mismatch**: Model output doesn't match seed expected values. Either update the seed CSV or fix the model logic. Query the CI test table on Dune to compare: `test_schema.git_dunesql_<hash>_<table>`.

### Manifest / Extra Models Running
- **More models running than expected**: The manifest file on main may be out of date. Check if the `commit manifest` workflow in Actions completed successfully. If it's still running or failed, wait for it to finish, then re-trigger CI.
- **Fix**: The Dune team's `commit manifest` workflow auto-runs after merges. If it failed, the Dune team needs to re-run it.

### Cluster Issues
- **DuneSQL cluster down**: The CI cluster may be offline. This requires the Dune team to fix internally. Check if other PRs are also failing.

## Step 4: Query CI Test Tables

CI tables are available on Dune for ~24 hours after the run:

```sql
select *
from test_schema.git_dunesql_<GIT_HASH>_<schema>_<alias>
limit 100
```

For Spellbook, the suffix matches dbt **`schema`** + **`_`** + **`alias`** (e.g. `tokens.transfers` → `tokens_transfers`). Use the exact name from **`dbt run initial model(s)`** logs.

Use these to:
- Verify data quality before expanding date ranges
- Compare CI output against production tables
- Debug seed test mismatches by joining CI table against the seed

If the CI table can't be found, the ~24-hour window may have elapsed. Re-run CI to rebuild.

## Step 5: Common Fixes

| Symptom | Likely Cause | Fix |
|---------|-------------|-----|
| "Metadata is not found" | Seed missing from prod and PR | Include seed file in PR with a small change |
| Duplicate rows in unique test | NULLs in unique key or bad join | Add `coalesce()` / fix join logic |
| Seed test values mismatch | Model logic changed or seed stale | Update seed CSV or fix model SQL |
| Extra models running | Manifest out of date | Wait for `commit manifest` workflow, re-run CI |
| Timeout at 90 min | Query too expensive | Add tighter date filters, check partition pruning |
| `prices.usd` token not showing | Backend backfill pending | Wait a few days for historical pricing to populate |
