# Sector-level Spell Design

Within Spellbook, there are two main spells which are considered the most popular and get the most usage – `dex.trades` and `nft.trades`. Both of these are considered sector-level spells. At the sector level, spells in this category typically span across many projects and blockchains. Due to these spells' heavy usage & importance to keep up-to-date, all sector-level spells follow a similar design pattern.

**Note**: not all sectors are up-to-date in the new structure, but will be considered moving forward

Since these sector-level spells will have their own dedicated `readme` within their directory with specifics, this will remain a high-level overview. Example dex readme [here](/models/_sector/dex/readme.md).

## Model level of granularity

Each model in Spellbook should follow a consistent pattern on level of granularity, where not all levels are required, but all fit into one of the levels. The full design pattern for level of granularity:

`project (version) → blockchain → cross-chain → sector/project spell`

**example**: `uniswap_v1_ethereum.base_trades → dex_ethereum.base_trades → dex.base_trades → dex.trades`

Not all spells will fit into a sector-wide downstream spell. Standalone spells will only fit into a few parts of the above format. When building, keep these principles in mind and submit as you see fit and the team will help validate as needed.

## Base-level spells within sector lineage

The upstream spells within the sector-level DBT lineages, as noted in the level of granularity above, are the project-level raw data, by version if applicable (i.e. `uniswap_v1_ethereum.base_trades`, `uniswap_v2_ethereum.base_trades`, etc). The following should be considered in design:

- Raw data only – base tables / decoded tables
  - The intention is to avoid materializing metadata which is later joined to enhance this raw data. materializing the metadata raises the risk of more frequent historical full refreshes on large amounts of data
  - To indicate raw data, the ‘base\_’ prefix is used on aliases
- Use macros for repeatable code – see [macro section](../macros/macro_overview.md) for more info
  - same projects cross-chain
  - forked projects on same chain
- Materialize and build incrementally
- Use universal unique keys across all upstream spells that feed into same sector spell
- Apply incremental filters on both source & target
  - Leverage the incremental predicate macros
- One schema file per blockchain
  - Ensure unique test applied to any materialized spells that contain unique keys
- One source file per blockchain
  - Table level of granularity is fine
  - Column level will provide more info to any future auto-generated docs
- With the idea of test-driven development in mind, build seed files and apply seed tests to each platform-level base spell
  - Expectation is to hardcode the spell with example outputs as expected, where the model runs and compares results against this file
- One blockchain-level spell which unions together all base-level spells on the chain
  - Views are fine here, as intention is for simplifying next steps vs. querying on the app
  - Leverage jinja for loops to iterate through the unions
- One base-level cross-chain spell, which simply unions the chain-level spells
  - Materialize this level to help performance on the last stage
- One final sector-level spell, with the base-level cross-chain spell passed into a final macro which enhances the data with metadata
