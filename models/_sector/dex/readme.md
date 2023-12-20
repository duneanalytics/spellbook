# README for models/_sector/dex/

Welcome to the `models/_sector/dex/` directory of our project. This document provides essential information on contributing to the dex sector models, along with insights into our design choices and the next steps in the dex.trades redesign workstream.

## How to Contribute

1. **Understanding the Structure**: Familiarize yourself with the current structure and conventions used in the dex sector models.
2. **Making Changes**: For any changes or additions, please fork the repository and create your feature branch from `main`.
3. **Pull Requests (PRs)**: Submit PRs with clear descriptions of changes and reference related issues or discussions.
4. **Code Review**: PRs will be reviewed by the team. Constructive feedback and suggestions are welcomed.

## Example PRs

- [PR #4929: Add mdex to dex.trades_beta](https://github.com/duneanalytics/spellbook/pull/4929/files)
- [PR #4924: Add biswap to dex.trades_beta](https://github.com/duneanalytics/spellbook/pull/4924/files)

## Dependency on dex.info

When adding a new dex to our project, it's important to add a corresponding entry in [`dex.info`](https://github.com/duneanalytics/spellbook/blob/main/models/dex/dex_info.sql). This step is crucial because `dex.info` serves as a central repository of metadata for all dexes, ensuring consistency and easy access to key information.

- [dex_info.sql](https://github.com/duneanalytics/spellbook/blob/main/models/dex/dex_info.sql)

Sample new entry:
```sql
, ('sushiswap', 'SushiSwap', 'Direct', 'SushiSwap')
```
## Adding Seed Tests

Seed tests play a vital role in our development process, adhering to the principles of test-driven development. They ensure that the output of our models aligns with the expected results, which are hardcoded in the seed. Here's how to add them:

1. **Define the Seed Schema**: Start by defining the schema of your seed in the [seeds/_sector/dex/_schema.yml](https://github.com/duneanalytics/spellbook/blob/main/seeds/_sector/dex/_schema.yml) file. This schema should match the expected output of your model.

  eg.
```yaml
  - name: trader_joe_avalanche_c_base_trades_seed
    config:
      column_types:
        blockchain: varchar
        project: varchar
        version: varchar
        tx_hash: varbinary
        evt_index: uint256
        block_number: uint256
        token_bought_address: varbinary
        token_sold_address: varbinary
        token_bought_amount_raw: uint256
        token_sold_amount_raw: uint256
        block_date: timestamp
```

2. **Add the Corresponding Seed File**: Create and add the seed file in the [seeds/_sector/dex/](https://github.com/duneanalytics/spellbook/tree/main/seeds/_sector/dex) directory. This file should contain the trades that verified on the blockchain explorers.
3. **Add Tests to Model Schema**: In your model's schema file, add tests that will validate the output against the seed data. These tests help to ensure data integrity and adherence to expected results.
Example tests:
```yaml
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - tx_hash
            - evt_index
      - check_dex_base_trades_seed:
          seed_file: ref('trader_joe_avalanche_c_base_trades_seed')
          filter:
            version: 2.1
```
- `dbt_utils.unique_combination_of_columns`: Ensures that the combination of specified columns is unique across the dataset.
- `check_dex_base_trades_seed`: Validates the model's output against the predefined seed data.

By following these steps, you can effectively implement seed tests to validate your models, ensuring that they produce the expected results.

## Project Structure Details

### Overview
The `dex.trades` redesign focuses on implementing improvements across all sectors, with an initial emphasis on `dex.trades`. This redesign aims to enhance the spell experience by incorporating advanced tech stack capabilities and addressing various opportunities for improvement.

### What's Changing and Why?
- **Base Project-Level Spells**: 
  - Will contain only data from raw & decoded source tables (e.g., [`uniswap_v1_ethereum.base_trades`](https://github.com/duneanalytics/spellbook/blob/main/models/_sector/dex/trades/ethereum/platforms/uniswap_v1_ethereum_base_trades.sql)).
  - This helps in keeping dbt lineages clean and CI/prod orchestration easy to support.
  - Materialized incrementally, these spells are not intended for end-user queries but as building blocks for `dex.trades`.

- **Chain Union Spells**: 
  - Will call a dbt macro to standardize a union of input spells on the same blockchain, forming another building block towards `dex_<blockchain>.base_trades`.

- **Sector Union Spell**: 
  - Calls a dbt macro to enrich the raw/decoded data with necessary metadata at `dex.trades` level.
  - Centralizes the logic and simplifies contributions of new projects.

### Key Initiatives
- **Naming Standards**: Adoption of the `base_` prefix for table aliases and `uniswap_` for macro names, indicating their role as building blocks.
- **Directory Structure**: 
  - Macros/Models: `macros/models/_sector/dex/`
  - Platforms: `models/_sector/dex/trades/<blockchain>/platforms/`
- **Materialization Strategy**: 
  - Platform/Project Level: Incremental.
  - Blockchain Level: View.
  - Sector Level: Incremental.
- **Model Config Settings**: 
  - Include both schema and alias, avoiding schema addition to project files.
  - Retain table configs as incremental/merge/delta.
  - Add incremental predicates for targeted filtering.

### Macro vs. Code in Model
- Standalone dexes can have code directly in the model.
- Forked dexes or repeatable logic should utilize or create macros.

Example [`uniswap_compatible_v2_trades` macro](https://github.com/duneanalytics/spellbook/blob/main/macros/models/_sector/dex/uniswap_compatible_trades.sql)
```sql
{{
    uniswap_compatible_v2_trades(
        blockchain = 'bnb',
        project = 'biswap',
        version = '2',
        Pair_evt_Swap = source('biswap_bnb', 'BiswapPair_evt_Swap'),
        Factory_evt_PairCreated = source('biswap_bnb', 'BiswapFactory_evt_PairCreated')
    )
}}
```

## Further Information

For more details, please refer to relevant GitHub issues, PRs, and comments. This repository is a collective effort, and your contributions are highly valued.
- [Initial PR #4533: SPE-200 restructuring dex.trades with a macro approach](https://github.com/duneanalytics/spellbook/pull/4533/files)
- [Github Issue #4759: migrate all dex models to the new structure / design](https://github.com/duneanalytics/spellbook/issues/4759)


## Contact

For any questions or discussions, feel free to reach out to [Jeff](https://github.com/jeff-dude) or [Hosuke](https://github.com/hosuke).
