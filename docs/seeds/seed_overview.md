# What are Seeds and How Should I Use Them?

Similar to macros, while there are various use cases for seeds within DBT, the main use case for wizards in Spellbook comes down to one approach. For sector-level spells, wizards should manually build a dataset in CSV format with expected results of a new model to enable a test-driven development approach.

Seeds will be required in these sector-level spell additions to ensure proper level of data quality is maintained. For standalone project spells, this is less of a requirement, although still encouraged for your own data quality.

## Using `dex.trades` as an Example for Sector-Level Spell Design Approach to Seeds:

1. Add new model seed to [the schema file](/seeds/_sector/dex/_schema.yml), to ensure proper data type assignments.
2. Build a seed file in CSV format, which contains:
   - All the unique keys on the model for downstream join conditions in tests.
   - Fields which we want to test the results of the model execution.
   - Example seed file [here](/seeds/_sector/dex/aerodrome_base_base_trades_seed.csv).
3. Within the [model schema file](/models/_sector/dex/trades/arbitrum/_schema.yml#L20-L23), call the [generic seed test](/tests/generic/check_dex_base_trades_seed.sql) with parameters necessary:
   - Seed file name.
   - Filter(s) for project versions, if the spell is split into versions per project.
4. Ultimately, following the above steps, the test query built and executed against seed files lives in the generic seed macro [here](/macros/test-helpers/check_seed_macro.sql).

## How Do I Track Seed Tests Running During Development?

The automated CI test which runs on each PR will execute the command to run & build seeds as needed, when present within a PR. After the seed builds, the models run to build spells in the CI test environment. After each is completed, the test(s) assigned in the model schema file will kick off the seed test, to compare data in the seed vs. the model output. If the PR CI test fails on testing phase for seed tests, that means either the seed needs fixed and/or the model logic is incorrect and needs updated.

For a deeper dive into how to read & use the attached CI tests to PRs, refer to [the CI docs](../ci_test/ci_test_overview.md).
