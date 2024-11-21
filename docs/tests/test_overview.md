# Tests Tied to Models

Tests tied to models are highly encouraged & at times required, depending on the spell involved. While there are a few required, there are also almost no limits to tests. If your spell contains complex logic to build and has heavy usage downstream on the Dune app, it can be helpful to apply tests upfront during development & over time as bugs are identified.

## Main Tests Used Universally Within Spellbook

- Unique test on column(s) used as unique keys in incremental models, to ensure no duplicates.
- Seed tests, as discussed more in detail in the [seeds directory](../seeds/seed_overview.md).

## Recent Tests Added

These tests would benefit all sector-level spells, first introduced in `nft.trades`:

- Validate column data types, as built [here](https://github.com/duneanalytics/spellbook/blob/d6b5acc1dbd01e67e6cb23d96da6f3fc3ec7d268/tests/generic/check_column_types.sql#L6) and called like [this](/models/_sector/nft/trades/chains/arbitrum/platforms/_schema.yml#L14).

## Where to Store Tests?

- Project-specific tests can live in their own directory, following ‘spellbook/test/<project>/’.
- Generic tests, which are applied universally and typically call a macro, must live in ‘spellbook/test/generic/’.
