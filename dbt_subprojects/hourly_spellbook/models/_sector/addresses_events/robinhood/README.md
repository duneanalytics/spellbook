# Robinhood funding deployment

Build and deploy `addresses_events_robinhood.first_funded_by` in hourly
Spellbook before the initial daily build of `addresses_robinhood.stats`. Daily
stats reads `source('addresses_events_robinhood', 'first_funded_by')`; dbt does
not enforce this dependency across the two projects. The daily CI job reads
production sources, so the hourly table must exist before daily CI can create
the stats model.

Robinhood uses the shared `addresses_events_first_funded_by` macro with native
ETH transfers from `tokens_robinhood.transfers`. The model keeps the standard
trace-address ordering, output columns, filters, null handling, and append-only
incremental behavior.
