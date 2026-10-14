# Arc funding deployment

Build and test `addresses_events_arc_first_funded_by` in hourly Spellbook before
the initial daily build of `addresses_arc_stats`. Daily stats reads
`source('addresses_events_arc', 'first_funded_by')`; dbt does not enforce that
dependency across the two projects. The production hourly table must exist
before daily stats can run, including daily CI that reads this source.

The model copies `addresses_events_first_funded_by` directly. Its only logic
change is to use `coalesce(evt_index, -1)` instead of the coalesced trace-address
array in transfer-ordering expressions. Arc native USDC transfers are EIP-7708
events with null trace addresses. All standard columns, filters, null handling,
and append-only incremental behavior remain unchanged.

The standard hourly `first_funded_by_seed` checks the funding result; the daily
`arc_first_funding_seed` remains the address-stats fixture. The hourly singular
test checks event ordering and transfer amounts within first-funding transactions
in the incremental window. Run the hourly seed and tests on both the initial
full-refresh build and a subsequent incremental build before enabling daily stats.
