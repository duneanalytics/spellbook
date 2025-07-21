# Onboarding New Blockchains to Spellbook

## Overview

This document outlines the standardized process for integrating new blockchains into Dune's Spellbook curated datasets. Proper integration ensures consistency and usability across Dune's platform. The process is typically broken down into three distinct Pull Requests (PRs) to manage complexity and facilitate review.

**Goal:** To systematically add blockchain metadata, core financial models (transfers, fees), and DEX data to Spellbook.

## Prerequisites

Before starting the Spellbook integration, the following must be in place:

1.  **Raw & Decoded Data:** The raw blockchain data (transactions, logs, traces, blocks) and any requested decoded contract data (e.g., for specific DEXs) must be available and visible in the Dune data explorer.
2.  **`dune.blockchains` Table:** The core `dune.blockchains` table must be updated with an entry for the new blockchain. This table serves as the source of truth for chain-level metadata. **Crucially, `chain_id` must be populated correctly.**
    -   ***Rationale:*** Many downstream models rely on this table, especially `chain_id`, to correctly identify and join data across different sources. Delays or inaccuracies here block subsequent steps.

## The Multi-PR Onboarding Process

We need to split the onboarding process into multiple PRs because of the way we have split up the Spellbook project into multiple subprojects. DBT can not model the dependencies between subprojects, so we need to do it manually. The CI results of steps 2 and 3 depend on the results of step 1, so we need to do it in separate PRs.

### PR 1: Foundational Metadata

**Goal:** Establish the core metadata for the blockchain, its native token, and essential ERC20 tokens within Spellbook. This lays the groundwork for all subsequent models.

**Steps:**

1.  **Add EVM Chain Info (`daily_spellbook`):**
    -   **What:** Add a row for the new blockchain to the [`evms.info`](./dbt_subprojects/daily_spellbook/models/evms/evms_info.sql) model in the [`dbt_subprojects/daily_spellbook/models/evms/`](./dbt_subprojects/daily_spellbook/models/evms/) directory.
    -   **Why:** This model centralizes EVM chain identifiers. The `chain_id` is particularly critical for linking automated token data sources later.
    -   *Example PR:* [Kaia Metadata PR](https://github.com/duneanalytics/spellbook/pull/6957/files) (Illustrates `evms.info` update)


2.  **Configure Token Prices (`tokens` project):**
    -   **What:**
        -   Add the native token to [`prices_native_tokens.sql`](./dbt_subprojects/tokens/models/prices/prices_native_tokens.sql) if it's not already present via another chain.
        -   Create a new `prices_<blockchain>.tokens` model (e.g., [`prices_lens_tokens.sql`](./dbt_subprojects/tokens/models/prices/lens/prices_lens_tokens.sql)) in `dbt_subprojects/tokens/models/prices/<blockchain>/`. Manually list the native token and other key tokens (e.g., top 5 transferred, stables, WETH).
        -   Add the new `prices_<blockchain>.tokens` model to the union in [`prices_tokens.sql`](./dbt_subprojects/tokens/models/prices/prices_tokens.sql).
        -  After this PR is merged we have to add the new chain to the `prices_trusted_tokens` pipeline on the sqlmesh project so our price feeds are updated.
    -   **Why:** Establishes price feeds for the native token and key ERC20s. We rely on CoinPaprika for feeds, so coverage might be limited initially.
    -   *Future State Note:* The dependency on CoinPaprika for *all* tokens is being reduced, but the `prices_trusted_tokens` pipeline will likely remain.

3.  **Configure ERC20 Metadata (`tokens` project) (not always required):**

*Note: This step is not required for all chains. It is only required for chains we could not generate amp coverage for.*

    -   **What:**
        -   Create a new `tokens_<blockchain>_erc20` model (e.g., [`tokens_lens_erc20.sql`](./dbt_subprojects/tokens/models/tokens/lens/tokens_lens_erc20.sql)) in `dbt_subprojects/tokens/models/tokens/<blockchain>/`. Add the same tokens listed in the blockchain-specific prices model.
        -   Add this new model to the union in [`tokens_erc20.sql`](./dbt_subprojects/tokens/models/tokens/tokens_erc20.sql).
    -   **Why:** Supplements the automated `dune.definedfi.dataset_tokens` source, which often lacks full blockchain or token coverage. Ensures essential tokens have metadata (symbol, decimals) available in Spellbook.

4.  **Define Raw Data Sources:**
    -   **What:** Add the new blockchain\'s raw tables (`transactions`, `logs`, `traces`, `blocks`, `creation_traces`) as sources in a new YAML file within [`sources/_base_sources/evm/`](./sources/_base_sources/evm/) (e.g., [`lens_base_sources.yml`](./sources/_base_sources/evm/lens_base_sources.yml)). Also, create a corresponding `<blockchain>_docs_block.md` file (e.g., [`lens_docs_block.md`](./sources/_base_sources/evm/lens_docs_block.md)) in the same directory to document these sources.
    -   **How:** 
        - You can either copy and replace an existing source file and docs and replace occurrences or create a new one.
        - You can also use the script `scripts/generate_evm_sources.py` and `scripts/generate_evm_docs.py` to generate the source file and docs. Simply replace the chain name in the script and run it.
    -   **Why:** Makes the raw data accessible within the dbt project, allowing models to reference them using the `source()` function.

5.  **Integrate into Aggregate EVM Models (`daily_spellbook`):**
    -   **What:** Update the `blockchains` list [here](../../dbt_subprojects/daily_spellbook/macros/helpers/evms_blockchains_list.sql)
    -   **Why:** Incorporates the new chain's base data (blocks, transactions, logs, etc.) into the primary cross-chain EVM tables used throughout Spellbook and Dune.

### PR 2: Core Transfer and Fee Models

**Goal:** Implement the fundamental [`tokens.transfers`](./dbt_subprojects/tokens/models/transfers_and_balances/tokens_transfers.sql) and [`gas.fees`](./dbt_subprojects/hourly_spellbook/models/_sector/gas/fees/gas_fees.sql) models for the new blockchain.

**Prerequisites:** PR 1 must be merged and deployed for the CI tests to successfully run.

**Steps:**

1.  **Implement Token Transfers (`tokens` project):**
    -   **What:**
        -   Create a `tokens_<blockchain>_base_transfers.sql` model using the [`transfers_base`](./dbt_subprojects/tokens/macros/transfers_base.sql) macro. **Carefully specify the correct native token address** (e.g., `0x000000000000000000000000000000000000800a` for Lens).
        -   Create the final enriched `tokens_<blockchain>_transfers.sql` model using the [`transfers_enrich`](./dbt_subprojects/tokens/macros/transfers_enrich.sql) macro.
        -   Create `tokens_<blockchain>_net_transfers_daily.sql` using the [`evm_net_transfers_daily`](./dbt_subprojects/tokens/macros/evm_net_transfers_daily.sql) macro.
        -   Create `tokens_<blockchain>_net_transfers_daily_asset.sql` using the [`evm_net_transfers_daily_asset`](./dbt_subprojects/tokens/macros/evm_net_transfers_daily_asset.sql) macro.
        -   Add the new `tokens_<blockchain>_transfers.sql` model to the union in the main [`tokens_transfers.sql`](./dbt_subprojects/tokens/models/transfers_and_balances/tokens_transfers.sql) model.
    -   **Why:** Creates the standardized token transfer table, combining native and ERC20 transfers and enriching them with USD values and metadata from PR 1. The `base_transfers` handles the core extraction, while `transfers_enrich` adds pricing and metadata. Net transfer models provide daily aggregates. Correct native token identification is critical and sometimes requires investigation (e.g., L2s might use specific contracts, not the zero address).
    -   *Example PR:* [Multi-chain Transfers PR](https://github.com/duneanalytics/spellbook/pull/7603/files)


2.  **Implement Gas Fees (`hourly_spellbook` project):**
    -   **What:**
        -   Create a `gas_<blockchain>_fees.sql` model in `dbt_subprojects/hourly_spellbook/models/_sector/gas/fees/<blockchain>/`. Adapt the standard EVM gas fee logic, potentially modifying the `evm_gas_fees` macro if the chain has unique fee mechanisms (e.g., OP Stack L1 fees, ZK rollups specificities).
        -   Add the new blockchain to the union in the main [`gas_fees.sql`](./dbt_subprojects/hourly_spellbook/models/_sector/gas/fees/gas_fees.sql) model.
        -   Add a few example transactions (including different tx types if applicable) for the new blockchain to the [`evm_gas_fees.csv`](./dbt_subprojects/hourly_spellbook/seeds/_sector/gas/evm_gas_fees.csv) seed file.
    -   **Why:** Provides a standardized view of transaction costs on the new chain. This is often the **most complex** part, requiring chain-specific research into how fees (base, priority, L1 data/blob fees for L2s) are calculated and recorded. Collaboration with GTM/chain experts is often necessary. The seed file ensures the `check_seed` data test passes.
    -   *Example PR:* [Base Gas Fees PR](https://github.com/duneanalytics/spellbook/pull/7635/files)

### PR 3: DEX Integration

**Goal:** Add abstractions for requested Decentralized Exchanges (DEXs) to the [`dex.trades`](./dbt_subprojects/dex/models/trades/dex_trades.sql) model.

**Prerequisites:** PR 1 must be merged and deployed. GTM team should have coordinated with the Chain team to provide necessary contract addresses and ensure decoding is working.

**Steps:**

1.  **Define DEX Sources:**
    -   **What:** Ensure the decoded event tables for the requested DEX (e.g., `Pair_evt_Swap`, `Factory_evt_PoolCreated`) are defined as sources in a relevant YAML file (e.g., [`sources/uniswap/lens/uniswap_v3_lens_sources.yml`](./sources/uniswap/lens/uniswap_v3_lens_sources.yml)).
    -   **Why:** Makes the decoded DEX event data accessible to the Spellbook models.

2.  **Build DEX Base Trades Model (`dex` project):**
    -   **What:**
        -   Create a new model in `dbt_subprojects/dex/models/trades/<blockchain>/platforms/` (e.g., [`uniswap_v3_lens_base_trades.sql`](./dbt_subprojects/dex/models/trades/lens/platforms/uniswap_v3_lens_base_trades.sql)).
        -   Use existing macros like [`uniswap_compatible_v3_trades`](./dbt_subprojects/dex/macros/models/_project/uniswap_compatible_v3_trades.sql) or [`uniswap_compatible_v2_trades`](./dbt_subprojects/dex/macros/models/_project/uniswap_compatible_v2_trades.sql) if the DEX is a known fork (e.g., Uniswap V2/V3).
        -   If it's a novel DEX design, significant custom modeling work might be required, often needing input from the DEX team via GTM.
        -   Add the new base model to the union in `dex_<blockchain>_base_trades.sql`.
        -   Create or update the corresponding DEX seed file (e.g., [`uniswap_lens_base_trades_seed.csv`](./dbt_subprojects/dex/seeds/trades/uniswap_lens_base_trades_seed.csv)) in [`dbt_subprojects/dex/seeds/trades/`](./dbt_subprojects/dex/seeds/trades/) with a few example trades.
    -   **Why:** Translates the raw DEX events into the standardized `dex.trades` schema columns. Reusing macros for known forks saves significant effort. The seed file ensures the `check_dex_base_trades_seed` data test passes.

3.  **Add to Aggregate `dex.trades`:**
    -   **What:** Add the `dex_<blockchain>_base_trades` view to the main [`dex.trades`](./dbt_subprojects/dex/models/trades/dex_trades.sql) union if the chain is intended for cross-chain DEX analysis.
    -   **Why:** Includes the new chain\'s DEX activity in the top-level `dex.trades` table used by many dashboards and queries.

- **Example PR:** [Sonic DEX PR](https://github.com/duneanalytics/spellbook/pull/7510/files)

## Post-Merge: Metrics Pipeline Integration

**Goal:** Integrate the new blockchain into the main Dune metrics pipeline, feeding the core Dune app dashboards.

**Prerequisites:** PR 2 ([`tokens.transfers`](./dbt_subprojects/tokens/models/transfers_and_balances/tokens_transfers.sql) and [`gas.fees`](./dbt_subprojects/hourly_spellbook/models/_sector/gas/fees/gas_fees.sql)) must be merged and live in production.

**Steps:**

1.  **Update Metrics Models (`hourly_spellbook` project):**
    -   **What:** Add logic for the new blockchain into the relevant metrics models within [`dbt_subprojects/hourly_spellbook/models/_sector/metrics/`](./dbt_subprojects/hourly_spellbook/models/_sector/metrics/). Mimic the patterns used for existing EVM chains.
    -   **Why:** Calculates standardized metrics (e.g., active users, transaction counts) based on the newly created [`tokens.transfers`](./dbt_subprojects/tokens/models/transfers_and_balances/tokens_transfers.sql) and [`gas.fees`](./dbt_subprojects/hourly_spellbook/models/_sector/gas/fees/gas_fees.sql) tables.
    -   *Example PR:* [zkSync Metrics PR](https://github.com/duneanalytics/spellbook/pull/7227/files)

2.  **Update Dune Materialized Views:**
    -   **What:** Manually edit the queries that generate the materialized views powering the Dune frontend metrics pages to include the new blockchain. These queries typically live in a specific Dune workspace folder. Refresh the materialized views.
    -   **Why:** The frontend application reads from these specific materialized views. They need to be explicitly updated to include the new chain's data after the underlying Spellbook models are live.
    -   *Location:* [Dune Prod Metrics Queries](https://dune.com/workspace/t/dune/library/folders/(PROD)%20Metrics%20pages) (Internal Link)

## Key Considerations

-   **Lead Time:** The more notice given before a chain launch, the better the Data team can prepare and allocate resources.
-   **Complexity & Uniqueness:** Each chain can have quirks (unique gas mechanisms, different native token handling, L2 specifics). Assume investigation time is needed.
-   **Data Quality:** Issues with raw data or decoding can block progress. Close collaboration with the platform and GTM teams is vital.
-   **ABI Accuracy:** Incorrect or missing ABIs for DEXs or other protocols will prevent accurate decoding and modeling. 