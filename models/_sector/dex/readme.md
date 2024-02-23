# README for `dex.trades`

Welcome to the `models/_sector/dex/` directory of our project. This README provides essential information on contributing to the `dex` sector models, insights into our design choices, and outlines the next steps in the `dex.trades` redesign workstream.

## Table of Contents

- [How to Contribute](#how-to-contribute)
- [Project Structure Details](#project-structure-details)
- [Adding New Sources](#adding-new-sources)
- [Defining Model Schemas](#defining-model-schemas)
- [Model Config Settings](#model-config-settings)
- [Example PRs](#example-prs)
- [Dependency on dex.info](#dependency-on-dexinfo)
- [Adding Seed Tests](#adding-seed-tests)
- [PR Submission Guidelines](#pr-submission-guidelines)
- [Further Information](#further-information)
- [Contact](#contact)

## How to Contribute

### Understanding the Structure

Familiarize yourself with the current structure and conventions used in the `dex` sector models.

### Making Changes

For any changes or additions, please fork the repository and create your feature branch from `main`.

### Pull Requests (PRs)

Submit PRs with clear descriptions of changes and reference related issues or discussions.

### Code Review

PRs will be reviewed by the team. Constructive feedback and suggestions are welcomed.

## Project Structure Details

### Overview

The `dex.trades` redesign focuses on implementing improvements across all sectors, with an initial emphasis on `dex.trades`. This redesign aims to enhance the spell experience by incorporating advanced tech stack capabilities and addressing various opportunities for improvement.

### Data Flow Architecture

Below is a diagram illustrating the `dex.trades` architecture. This visual guide helps to understand how base trades compatible macros are used to feed into our `dex.trades` data model, showing the flow from source tables (like decoded swap and pair created event source tables) to the final enriched `dex.trades` view. It also highlights the integration of various chain-specific base trades and the enrichment process through macros.

![DEX.Trades Architecture Diagram](https://github.com/duneanalytics/spellbook/assets/102681548/236e0920-8073-44c9-9cde-e0219d236101)

### Core Components

- **Base Project-Level Spells**: Contain only data from raw & decoded source tables, serving as building blocks for `dex.trades`.
- **Chain Union Spells**: Utilize a dbt macro to standardize a union of input spells on the same blockchain.
- **Sector Union Spell**: Enriches the raw/decoded data with necessary metadata at the `dex.trades` level.

### Naming Standards

Adoption of the `base_` prefix for table aliases and `uniswap_` for macro names.

### Directory Structure

- Macros/Models: `macros/models/_sector/dex/`
- Platforms: `models/_sector/dex/trades/<blockchain>/platforms/`

### Materialization Strategy

- Platform/Project Level: Incremental.
- Blockchain Level: View.
- Sector Level: Incremental.

### Macro vs. Code in Model

- Standalone dexes can have code directly in the model.
- Forked dexes or repeatable logic should utilize or create macros.

### Macro Usage Example

One of the key components in the dex.trades redesign is the utilization of dbt macros to standardize and simplify the process of data transformation across different blockchains and projects. Below is an example of how to use the [`uniswap_compatible_v2_trades` macro](/macros/models/_sector/dex/uniswap_compatible_trades.sql) macro within our project. This macro is designed to standardize the trades data for projects compatible with Uniswap V2 on various blockchains.

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

## Adding New Sources

When incorporating new data sources into the `dex` sector, it's essential to properly define them in our dbt project. Here’s how to add new sources:

1. **Locate the Source YML File**: Navigate to `sources/_sector/dex/trades/[blockchain]` in the project directory.

2. **Edit the `_sources.yml` File**: Within this file, you’ll define the new source tables. Provide the necessary details such as name, description, database, schema, and table identifier. Here’s an example:

```yaml
- name: clipper_ethereum
  tables:
    - name: ClipperExchangeInterface_evt_Swapped
    - name: ClipperCaravelExchange_evt_Swapped
    - name: ClipperVerifiedCaravelExchange_evt_Swapped
    - name: ClipperApproximateCaravelExchange_evt_Swapped
```

## Defining Model Schemas

For each model in the DEX sector, we must define its schema. This schema outlines the structure of the model and the definitions of its columns. Here’s how to add a schema for a model:

1. **Locate the Schema YML File**: Go to `models/_sector/dex/trades/[blockchain]` in the project directory.

2. **Edit the `_schema.yml` File**: Add the schema definition for your model. This includes specifying column names, types, descriptions, and any tests that should be applied to the columns. For example:

```yaml
- name: uniswap_v2_ethereum_base_trades
  meta:
    blockchain: ethereum
    sector: dex
    project: uniswap
    contributors: jeff-dude, masquot, soispoke, hosuke
  config:
    tags: ["ethereum", "dex", "trades", "uniswap", "v2"]
  description: "uniswap ethereum v2 base trades"
  tests:
    - dbt_utils.unique_combination_of_columns:
        combination_of_columns:
          - tx_hash
          - evt_index
    - check_dex_base_trades_seed:
        seed_file: ref('uniswap_ethereum_base_trades_seed')
        filter:
          version: 2
```

## Model Config Settings

- Include both schema and alias, avoiding schema addition to project files.
- Retain table configs as incremental/merge/delta.
- Add incremental predicates for targeted filtering.

Example config block:

```yaml
{{ config(
    schema = 'uniswap_v2_ethereum'
    , alias = 'base_trades'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['tx_hash', 'evt_index']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}
```

## Example PRs

- [PR #4929: Add mdex to dex.trades_beta](https://github.com/duneanalytics/spellbook/pull/4929/files)
- [PR #4924: Add biswap to dex.trades_beta](https://github.com/duneanalytics/spellbook/pull/4924/files)

## Dependency on dex.info

It's crucial to add a corresponding entry in [`dex.info`](/models/dex/dex_info.sql) when adding a new `dex`.

Sample new entry:

```sql
, ('sushiswap', 'SushiSwap', 'Direct', 'SushiSwap')
```

## Adding Seed Tests

Seed tests are vital for ensuring that the output of our models aligns with the expected results.

### Define the Seed Schema

Start by defining the schema of your seed in the [seeds/\_sector/dex/\_schema.yml](/seeds/_sector/dex/_schema.yml) file.
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

### Add the Corresponding Seed File

Create and add the seed file in the [seeds/\_sector/dex/](/seeds/_sector/dex) directory. This file should contain the trades that verified on the blockchain explorers.

### Add Tests to Model Schema

In your model's schema file, add tests that validate the output against the seed data. These tests help to ensure data integrity and adherence to expected results.

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

## PR Submission Guidelines

When contributing new dexes to the `dex.trades`, here are a few guidelines to ensure a smooth submission process:

- **Multiple dexes on the Same Chain**: If you're adding multiple dexes that operate on the same blockchain, it's best to group them together in the same Pull Request (PR). This helps in consolidating the review process and ensures consistency across dex integrations on the same chain.

- **Different Chains**: For dexes operating on different blockchains, please submit a separate PR for each chain. This approach not only makes it easier to manage and review changes specific to each blockchain's unique characteristics and requirements but also significantly reduces the potential for file conflicts when updates are merged into the main branch.

Following these guidelines helps maintain the spellbook's organization and facilitates efficient review and integration of your contributions.

## Further Information

For more details, please refer to relevant GitHub issues, PRs, and comments. This repository is a collective effort, and your contributions are highly valued.

- [Initial PR #4533: SPE-200 restructuring dex.trades with a macro approach](https://github.com/duneanalytics/spellbook/pull/4533/files)
- [Github Issue #4759: migrate all dex models to the new structure / design](https://github.com/duneanalytics/spellbook/issues/4759)

## Contact

For any questions or discussions, feel free to reach out to [Jeff](https://github.com/jeff-dude) or [Hosuke](https://github.com/hosuke).
