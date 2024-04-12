# Macros in DBT for Spellbook

There are various use cases for macros in DBT, but the main focus for wizards in Spellbook is to host reusable code across multiple models.

## Directory Path for Macros

- Path: `spellbook/macros/models/`
- Within this path, there are two main subdirectories:

  1. **\_project**
     - For standalone project spell lineages.
  2. **\_sector**
     - For sector-level spell lineages, e.g., `dex.trades` lineage.

  - **Note**: The ‘\_’ prefix in directory paths is used to keep them at the top & easily accessible, reserved for the most frequently used directories.

## Design Principles for Macros

Following [this](/macros/models/_sector/dex/uniswap_compatible_trades.sql) example, where uniswap compatible projects code lives, the key design principles include:

- **Assign Arguments**
  - Define arguments expected to be passed in via each model that calls the macro.
- **Parameterization**
  - Ensure high flexibility to allow various models to call the macro.
- **Best Practices**
  - Continue to apply best practices in model code design.
- **Multiple Macros in One File**
  - It's common to have multiple macros within a single file, such as various versions of the uniswap contract code. Group similar macros together logically.

Within models, such as uniswap v2, call macro code with [this approach](/models/_sector/dex/trades/ethereum/platforms/uniswap_v2_ethereum_base_trades.sql).

## When to Use a Macro

- **Repeated Code**
  - Use macros for spells that contain repeatable code beneficial for other spells.
- **Examples**
  - Project contracts forked on the blockchain without significant changes.
  - Project contracts that span multiple compatible blockchains.

## Benefits of Macros

- **Centralized Code Modifications**
  - Modifications occur in one place, reducing bugs across spells.
- **Data Quality and Consistency**
  - Maintain a high level of data quality and consistency across spells.

## Other Macro Use Cases

1. **Generic Test Queries**
   - Seed tests within model schema files call a seed macro containing the test query.
2. **Universal Use Cases in Spellbook**
   - incremental predicates, containing the incremental filter which can be controlled in one location and called across multiple models.
   - incremental days forward, which can chunk data from the source into smaller time frames to help bypass performance limitations.
   - Macros with lists for for-loops in models, like [`all_evm_chains`](/macros/public/all_evm_chains.sql).
3. **Dune Team Specific Cases**
   - Overriding dbt-trino core macros for Spellbook-specific scenarios.
   - Backend database specific code in pre or post hooks for spell optimization.

## Future Developments

- **Note**: Currently, Spellbook does not support user-defined functions built via macros on the backend database. This feature is planned for a future product release to unlock more use cases.
