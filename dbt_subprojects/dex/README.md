## DEX subproject

This is a DBT subproject for the main lineages of the DEX sector. Included in this subproject, but not limited to over time:
- `dex.trades`
- `dex_aggreagtor.trades`
- `dex.prices`
- `dex.sandwiches`
- `dex.sandwiched`
- ...and more!

This subproject will be dedicated to building the above spells (and others in the future related to DEX) on an hourly cadence in production. All other spells not included within this subproject will treat these spells as sources. For example, labels spells which read from `dex.trades` will now treat the spell as a source, rather than reference within dbt.

