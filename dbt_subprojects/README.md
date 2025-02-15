# DBT Subprojects

This directory contains various DBT subprojects, each focusing on different aspects of blockchain data:

## Project Structure

- `daily_spellbook/` - Daily transformations and data aggregations
- `hourly_spellbook/` - Hourly transformations for data requiring more frequent updates
- `dex/` - Models related to decentralized exchanges
- `nft/` - Transformations for NFT data
- `solana/` - Solana blockchain specific models
- `tokens/` - Models for token operations and metrics

## Usage

Each subproject can be run independently using the DBT CLI:

```bash
dbt run --project-dir dbt_subprojects/<subproject_name>
```

For example, to run daily_spellbook:
```bash
dbt run --project-dir dbt_subprojects/daily_spellbook
```

## Development

When creating new models:
1. Choose the appropriate subproject based on data type and update frequency
2. Follow the existing model structure in the chosen subproject
3. Ensure all dependencies are properly defined in `ref()` macros

## Additional Information

Each subproject contains its own `dbt_project.yml` and may have specific configurations and dependencies. Please refer to the individual subproject documentation for more details.
