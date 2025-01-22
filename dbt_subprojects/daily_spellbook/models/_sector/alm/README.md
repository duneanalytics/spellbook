# README for `alm.trades`

Welcome to the `models/_sector/alm/` directory of our project. This README provides essential information on contributing to the `alm` sector models, insights into our design choices, and outlines the next steps in the `alm.trades` redesign workstream.

## Table of Contents

- [How to Contribute](#how-to-contribute)
- [Project Structure Details](#project-structure-details)
- [Adding New Sources](#adding-new-sources)
- [Defining Model Schemas](#defining-model-schemas)
- [Model Config Settings](#model-config-settings)
- [Example PRs](#example-prs)
- [PR Submission Guidelines](#pr-submission-guidelines)
- [Further Information](#further-information)
- [Contact](#contact)

## How to Contribute

### Understanding the Structure

Familiarize yourself with the current structure and conventions used in the `alm` sector models.

### Making Changes

For any changes or additions, please fork the repository and create your feature branch from `main`.

### Pull Requests (PRs)

Submit PRs with clear descriptions of changes and reference related issues or discussions.

### Code Review

PRs will be reviewed by the team. Constructive feedback and suggestions are welcomed.

## Project Structure Details

### Overview

Similar to `dex.trades` this model aims to easily serve data regarding the subset of traded volume that were served by liquidity managed by protocols such as Arrakis Finance, or Gamma, etc.
Because of that, the `alm.trades` design mimics the one used for `dex.trades`.

### Data Flow Architecture

As previously said, the architecture of `alm.trades` mimics the one of `dex.trades`. Because of that, [this diagram](https://github.com/duneanalytics/spellbook/blob/main/dbt_subprojects/dex/models/trades/readme.md) can be taken as a reference.
In `alm.trades`, the 2 macros used are:
- `arrakis_compatible_v2_trades`: which tracks all the Uniswap V3 LP positions (timestamp, liquidity, and tick information) minted by Arrakis Finance vaults, and then derives the volume served for each swap based on the price movement of the pool.
- `add_pool_price_usd`: which uses `prices.usd` to populate the pool price in USD, so the volume served can be expressed in USD terms.

### Core Components

- **Base Project-Level Spells**: Contain only data from raw & decoded source tables, serving as building blocks for `alm.trades`.
- **Chain Union Spells**: Utilize a dbt macro to standardize a union of input spells on the same blockchain.
- **Sector Union Spell**: Enriches the raw/decoded data with necessary metadata at the `alm.trades` level.

### Naming Standards

Adoption of `arrakis_` for macro names.

### Directory Structure

- Macros/Models: `macros/models/_sector/alm/`
- Platforms: `models/_sector/alm/trades/<blockchain>/platforms/`

### Materialization Strategy

- Platform/Project Level: Incremental.
- Blockchain Level: View.
- Sector Level: Incremental.

### Macro vs. Code in Model

- Standalone protocols can have code directly in the model.
- Those protocols which are forks, or deployed in multiple chains, or with repeatable logic, should utilize or create macros.

### Macro Usage Example

Below is an example of how to use the [`arrakis_compatible_v2_trades` macro](/dbt_macros/models/_sector/alm/arrakis_compatible_trades.sql) macro within our project. This macro is designed to standardize the trades data for projects compatible with Arrakis V2 on various blockchains.

```sql
{{
    arrakis_compatible_v2_trades(
        blockchain = 'arbitrum',
        project = 'arrakis',
        version = '2',
        dex = 'uniswap',
        dex_version = '3',
        Pair_evt_Mint = source('uniswap_v3_ethereum', 'Pair_evt_Mint'),
        Pair_evt_Burn = source('uniswap_v3_ethereum', 'Pair_evt_Burn'),
        Pair_evt_Swap = source('uniswap_v3_ethereum', 'Pair_evt_Swap'),
        Factory_evt_PoolCreated = source('uniswap_v3_ethereum', 'Factory_evt_PoolCreated'),
        ArrakisV2Factory_evt_VaultCreated = source('arrakis_finance_ethereum', 'ArrakisV2Factory_evt_VaultCreated')
    )
}}
```

## Adding New Sources

When incorporating new data sources into the `alm` sector, it's essential to properly define them in our dbt project. Here’s how to add new sources:

1. **Locate the Source YML File**: Navigate to `sources/_sector/alm/[blockchain]` in the project directory.

2. **Edit the `_sources.yml` File**: Within this file, you’ll define the new source tables. Provide the necessary details such as name, description, database, schema, and table identifier. Here’s an example:

```yaml
version: 2

sources:
  - name: arrakis_v2_ethereum
  - name: uniswap_v3_ethereum
```

## Defining Model Schemas

For each model in the DEX sector, we must define its schema. This schema outlines the structure of the model and the definitions of its columns. Here’s how to add a schema for a model:

1. **Locate the Schema YML File**: Go to `models/_sector/dex/trades/[blockchain]` in the project directory.

2. **Edit the `_schema.yml` File**: Add the schema definition for your model. This includes specifying column names, types, descriptions, and any tests that should be applied to the columns. For example:

```yaml
version: 2

models:
  - name: alm_ethereum_trades
    meta:
      blockchain: ethereum
      sector: alm
      contributors: 0xrusowsky
    config:
      tags: [ 'ethereum', 'alm', 'trades' ]

  - name: arrakis_finance_ethereum_trades
    meta:
      blockchain: ethereum
      sector: alm
      project: arrakis_finance
      contributors: 0xrusowsky
    config:
      tags: [ 'ethereum', 'alm', 'arrakis', 'arrakis_finance', 'trades' ]
    description: "arrakis finance ethereum base trades"
    data_tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - tx_hash
            - evt_index
            - vault_address
```

## Model Config Settings

- Include both schema and alias, avoiding schema addition to project files.
- Retain table configs as incremental/merge/delta.
- Add incremental predicates for targeted filtering.

Example config block:

```yaml
{{ config(
    schema = 'arrakis_finance_ethereum'
    , alias = 'trades'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['tx_hash', 'evt_index', 'vault_address']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}
```

## Example PRs

- [PR #5764: setup ALM trades + feed Arrakis data](https://github.com/duneanalytics/spellbook/pull/5764/files)

## PR Submission Guidelines

When contributing new protocols to the `alm.trades`, here are a few guidelines to ensure a smooth submission process:

- **Multiple protocols on the Same Chain**: If you're adding multiple protocols that operate on the same blockchain, it's best to group them together in the same Pull Request (PR). This helps in consolidating the review process and ensures consistency across integrations on the same chain.

- **Different Chains**: For protocols operating on different blockchains, please submit a separate PR for each chain. This approach not only makes it easier to manage and review changes specific to each blockchain's unique characteristics and requirements but also significantly reduces the potential for file conflicts when updates are merged into the main branch.

Following these guidelines helps maintain the spellbook's organization and facilitates efficient review and integration of your contributions.

## Further Information

For more details, please refer to relevant GitHub issues, PRs, and comments. This repository is a collective effort, and your contributions are highly valued.

- [PR #5764: setup ALM trades + feed Arrakis data](https://github.com/duneanalytics/spellbook/pull/5764/files)

## Contact

For any questions or discussions, feel free to reach out to [Jeff](https://github.com/jeff-dude), [Hosuke](https://github.com/hosuke), or [0xrusowsky](https://github.com/0xrusowsky).
