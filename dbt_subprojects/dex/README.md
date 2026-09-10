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

`dex.trades` consumers merge on `(blockchain, tx_hash, evt_index)`, so it has to identify
a single trade. Projects decoded from **event logs** carry the real log index and are fine.
Projects decoded from **call traces** have no log index to carry; numbering them `1..n` per
transaction puts them in the same number space as real log indices and they collide. On
2026-09-09 an `origin_arm` swap numbered 2 collided with a `uniswap` v4 swap whose real log
index was 2, and the duplicate key stalled the prices pipeline for 15 hours.

Prefer a real log index whenever the venue emits an event — `curve` and `ekubo` recover one
by matching their call traces back to the swap logs. Only when there is nothing to anchor to,
offset the synthetic numbering into a band of its own. Real log indices peaked at 25,509 over
the last 90 days, so bands start far above that:

| band | project |
| --- | --- |
| 101,000,000 | `origin_arm` (ethereum) |
| 102,000,000 | `tempo_exchange` (tempo) |
| 104,000,000 | `zigzag` (arbitrum) |
| 105,000,000 | `angstrom` (ethereum) |

Take an unused band for a new trace-derived project — a band is 1,000,000 wide, which is far
more than the trades one transaction can hold. Two projects sharing a band collide only if
they appear in the same transaction, and the `unique_combination_of_columns` test on
`dex.trades` is what catches it.

103,000,000 is reserved for `1inch-LOP`, whose fills are numbered by
`oneinch_lop_evt_index()` and still collide with real log indices; renumbering it spans every
chain, so it is deferred to its own change.

Changing a project's numbering needs a **full refresh** of its model: the incremental merge
key is `(tx_hash, evt_index)`, so a plain re-run inserts renumbered rows alongside the old
ones instead of replacing them.

