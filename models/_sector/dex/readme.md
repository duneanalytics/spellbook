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

## Directory Structure

- Macros/Models: `macros/models/_sector/dex/`
- Platforms: `models/_sector/dex/trades/platforms/`

## Materialization Strategy

- Platform/Project Level: Incremental
- Blockchain Level: View
- Sector Level: Incremental

## Model Configuration Settings

- Include both schema and alias; avoid adding schema to project file.
- Maintain table configs (incremental/merge/delta).
- Add incremental predicates for targeted filtering on incremental loads.
eg.
```sql
{{
    config(
        schema = 'biswap_v2_bnb',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}
```

## Macro vs. Code in Model

- Standalone dex: Code directly in model is acceptable.

- Forked dex/Repeatable logic: Use or create a macro. eg.
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

## Contact

For any questions or discussions, feel free to reach out to [Jeff](https://github.com/jeff-dude) or [Hosuke](https://github.com/hosuke).
