## DEX subproject


This is a DBT subproject for the main lineages of the DEX sector. Included in this subproject, but not limited to over time:
- `dex.trades`
- `dex_aggreagtor.trades`
- `dex.prices`
- `dex.sandwiches`
- `dex.sandwiched`
- ...and more!

This subproject will be dedicated to building the above spells (and others in the future related to DEX) on an hourly cadence in production. All other spells not included within this subproject will treat these spells as sources. For example, labels spells which read from `dex.trades` will now treat the spell as a source, rather than reference within dbt.

## Synthetic `evt_index` bands

Consumers of `dex.trades` merge on `(blockchain, tx_hash, evt_index)`. Projects decoded from
event logs carry the real log index; projects decoded from call traces have none, and
numbering them `1..n` collides with the log index of an event-based project in the same
transaction. That is what stalled the prices pipeline for 15 hours on 2026-09-09.

Recover a real log index where the venue emits an event — `curve` and `ekubo` match their
call traces back to the swap logs. Only when there is nothing to anchor to, offset the
numbering into a band. Real log indices peaked at 25,509 over the last 90 days:

| band | project |
| --- | --- |
| 101,000,000 | `origin_arm` (ethereum) |
| 102,000,000 | `tempo_exchange` (tempo) |
| 103,000,000 | reserved for `1inch-LOP`, still unbanded |
| 104,000,000 | `zigzag` (arbitrum) |
| 105,000,000 | `angstrom` (ethereum) |

Take an unused band for a new project; the `unique_combination_of_columns` test on
`dex.trades` catches a reused one. Changing a project's numbering needs a full refresh —
the merge key is `(tx_hash, evt_index)`, so a plain re-run inserts renumbered rows alongside
the old ones.
