# Same-block sandwich correction

Both `dex_sandwiches` and `dex_sandwiched` must use block numbers as well as
timestamps. Event indices are block-local. Equal timestamps alone permit
cross-block attacker pairs and victims, including when the victim model
reconstructs bounds from otherwise valid attacker trades.

## Validation on 2026-09-09

- Local dbt parse and compile of both BNB models and the regression test pass.
  The rebuild selector resolves to exactly 34 output models in the local
  manifest. Compile used `--no-introspect --no-populate-cache`; production
  model execution and post-build table validation are still outstanding.
- [DuneSQL regression fixture](https://dune.com/queries/8656063): zero failures.
  Covers a valid sandwich, different-block backrun, different-block victim,
  two valid sandwiches sharing timestamp/pool/attacker, and unrelated trades
  inside false victim bounds. Also checks missing rows and duplicates.
- [Mutation checks](https://dune.com/queries/8656067): restoring the old behavior
  produces nine failures. Removing each of the four block guards independently
  produces failures (2, 2, 1, and 3 respectively for attacker pairing, attacker
  victim matching, victim-bound pairing, and victim-bound trade matching).
- [Shared timestamp scan](https://dune.com/queries/8656052), 2026-09-08 UTC:
  BNB 86,325; Arbitrum 32,822; Avalanche 527; Scroll 410; zkSync 162;
  Fantom 133; Sei 70 timestamps contain trades from multiple blocks. This
  measures exposure, not false positives, and only examines blocks with trades.
- [Detection comparison](https://dune.com/queries/8656066), 2026-09-08
  12:00–13:00 UTC: BNB 5,991 old vs 3,009 corrected distinct attacker trade
  rows, removing 2,982 (49.7747%). Known-price attacker volume changes from
  $1,320,000.18 to $625,474.26; 129 old rows have no USD price.
  This is **before transaction enrichment and whitelist exclusion**, not a
  production table diff or a count of attacks/victims. Do not extrapolate it
  to all BNB history. SQL is in `dex_sandwich_same_block_impact.sql`.
- The same hour covers 15 chains with trades: only BNB loses detector rows.
  Zora has no source trades in that window. Zero detections or no change in this
  hour do not establish that a chain is unaffected historically.

## Historical correction still required

There are 16 chain-level attacker tables and 16 chain-level victim tables,
plus the two incremental combined tables `dex.sandwiches` and `dex.sandwiched`.
Chains: arbitrum, avalanche_c, bnb, ethereum, fantom, gnosis, optimism, polygon,
base, celo, zksync, scroll, zora, unichain, sei, mantle. Solana has separate
macros/models and is outside this correction.

All 34 outputs use `incremental_strategy='merge'`. The merge implementation
updates or inserts source rows; it does not delete target rows absent from the
new source. Neither the normal three-day run nor a wider incremental run will
remove stale false positives, even inside the run's time window.

The sandbox does not have the production Trino host/routing/credentials. No
production rebuild, deletion, or deployment was executed. Run these steps with
the production orchestrator after review, CI, and merge of the correction:

1. Before replacement, record counts, distinct trade keys, USD volumes and
   missing-price counts by chain/day for all 34 tables. Retain snapshots or
   another recoverable before-state. Identify customer dashboards and saved
   queries outside dbt that consume these tables.
2. Extend the bounded comparison to representative dates across history and all
   16 chains. Compare final post-whitelist attacker output and victim output;
   keep detector-only results separate. Determine the affected time range from
   data instead of assuming a chain upgrade date.
3. Coordinate with the owner of the scheduled DEX job so scheduled merges cannot
   race table replacement. Check source history completeness first.
4. Preview the exact selector with the production profile. It must contain only
   the 34 attacker/victim output models, not their large upstream trade models
   or the whitelist/tag models:

   ```sh
   uv run dbt ls --project-dir dbt_subprojects/dex \
     --profiles-dir dbt_subprojects/dex --profile spellbook_dex --target prod \
     --select path:models/sandwiches \
     --exclude dex_sandwiches_whitelist dex_sandwiches_whitelist_tags \
     --resource-type model --output name
   ```

5. Rebuild the selected tables with full refresh (not an ordinary merge).
   dbt's dependency graph builds chain attackers before their victims, and
   the chain tables before the corresponding combined table. This replaces
   stored contents and explicitly removes rows that no longer qualify.

   ```sh
   uv run dbt build --project-dir dbt_subprojects/dex \
     --profiles-dir dbt_subprojects/dex --profile spellbook_dex --target prod \
     --select path:models/sandwiches \
     --exclude dex_sandwiches_whitelist dex_sandwiches_whitelist_tags \
     --full-refresh --vars '{dev_dates: false}'
   ```

   A staged table swap is an alternative for availability/cost reasons, but it
   must replace the affected history in both chain and combined tables and
   recompute victims from corrected attackers. A merge-only backfill is not an
   alternative. Coordinate atomicity/availability across these table swaps;
   dbt does not replace all 34 tables in one transaction.
6. Compare retained, removed, and added trade keys against the before-state,
   including periods older than the rolling window. Investigate unexpected
   additions/source changes. Verify same-block witnesses for retained attacker
   rows and same-block bounds for victims, whitelist behavior, key uniqueness,
   and combined-versus-chain union parity. Save the queries and counts.
7. Resume the scheduled job, validate an incremental cycle, and verify the
   customer-visible `dex.sandwiches` and `dex.sandwiched` results before closing
   the incident. Do not infer completion from successful dbt execution alone.

The correction keeps the published columns and keys unchanged. Leg labels and
a stable pairing ID remain a separate interface/design question; this change
does not add them or alter other sandwich heuristics.
