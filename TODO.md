# Spellbook Model Cleanup Progress

## Summary
Removing 94 unused/lightly used models from the dbt project based on cleanup_candidates_20250610_163555.md

**Total models to remove:** 91 (adjusted: 3 models restored due to dependencies/active maintenance)
**Models processed:** 79 (3 restored due to dependencies/active maintenance)
**Models remaining:** 12

## ⚠️ Model Restoration Notices

### **labels.ofac_sanctioned_ethereum** - RESTORED (not eligible for removal)
- **Reason**: Referenced by `labels_addresses.sql` - has active dependencies
- **Files restored**:
  - `dbt_subprojects/daily_spellbook/models/_sector/labels/addresses/__single_category_labels__/ofac_sanctionned/labels_ofac_sanctionned_ethereum.sql`
  - `dbt_subprojects/daily_spellbook/models/_sector/labels/addresses/__single_category_labels__/ofac_sanctionned/labels_ofac_sanctionned_ethereum_schema.yml`

### **evms.erc1155_approvalsforall & evms.erc721_approvalsforall** - RESTORED (not eligible for removal)
- **Reason**: Models were actively modified in main branch, indicating ongoing maintenance and usage
- **Files restored**:
  - `dbt_subprojects/daily_spellbook/models/evms/evms_tables/evms_erc1155_approvalsforall.sql`
  - `dbt_subprojects/daily_spellbook/models/evms/evms_tables/evms_erc721_approvalsforall.sql`
  - Schema entries in `dbt_subprojects/daily_spellbook/models/evms/evms_schema.yml`
- **Merge Resolution**: Resolved conflicts by accepting main branch versions during merge with origin/main

## Subproject breakdown:
- **daily_spellbook**: 63 tables (3 restored)
- **hourly_spellbook**: 26 tables  
- **dex**: 2 tables
- **nft**: 2 tables

## Compilation Validation

### ✅ All Subprojects Compiling Successfully

**Latest validation results (Post-Models 31-40 cleanup):** All 6 subprojects compile without errors or warnings.

✅ **tokens**: Compiled successfully (389 models, 508 data tests, 7192 sources)
✅ **solana**: Compiled successfully (225 models, 328 data tests, 37 seeds, 7251 sources)  
✅ **nft**: Compiled successfully (302 models, 681 data tests, 48 seeds, 7200 sources)
✅ **hourly_spellbook**: Compiled successfully (1360 models, 4734 data tests, 134 seeds, 7209 sources)
✅ **dex**: Compiled successfully (1038 models, 411 seeds, 1903 data tests, 7192 sources)
✅ **daily_spellbook**: Compiled successfully (1833 models, 2303 data tests, 47 seeds, 7202 sources)

### Fixes Applied During Validation:
- **tokemak_ethereum_schema.yml**: Removed empty schema file causing parsing errors
- **evms_schema.yml**: Fixed YAML syntax error in topic0 description (quote escaping)
- **evms_schema.yml**: Fixed undefined alias `*approved` by adding proper field definition
- **dbt_project.yml**: Removed unused tokemak configuration paths

### How to Validate Changes with dbt --warn-error compile

After making model deletions, always validate that all subprojects still compile correctly:

**Command:**
```bash
dbt --warn-error compile
```

**Usage Instructions:**
1. Navigate to each subproject directory:
   ```bash
   cd dbt_subprojects/<subproject_name>
   ```

2. Run the compile command with warn-error flag:
   ```bash
   dbt --warn-error compile
   ```

3. The `--warn-error` flag ensures that warnings are treated as errors, providing stricter validation

**Example workflow for all subprojects:**
```bash
# From project root
cd dbt_subprojects/tokens && dbt --warn-error compile
cd ../solana && dbt --warn-error compile  
cd ../nft && dbt --warn-error compile
cd ../hourly_spellbook && dbt --warn-error compile
cd ../dex && dbt --warn-error compile
cd ../daily_spellbook && dbt --warn-error compile
```

**What to expect:**
- ✅ **Exit code 0**: Compilation successful, no errors or warnings
- ❌ **Exit code 1/2**: Compilation failed, fix issues before proceeding
- Look for model counts in output to verify expected totals

**Recent Fix Applied:**
- **daily_spellbook**: Fixed missing `feed_address` anchor definition in `chainlink_schema.yml`
- Added missing YAML anchor: `&feed_address` with name and description

## ⚠️ IMPORTANT: Test Cleanup Required
**Always check for and remove tests that reference deleted models**
- Search for test files in `tests/` directories that reference the model being deleted
- Look for patterns like `{{ ref('model_name') }}` in test files
- Delete any tests that exclusively test the deleted model
- Update tests that reference the deleted model along with other models

**Example fixes completed:**
- ❌ **aave_interests_test.sql**: Referenced deleted `aave_v2_ethereum_interest_rates` - DELETED
- File: `dbt_subprojects/hourly_spellbook/tests/_project/aave/ethereum/aave_interests_test.sql` - REMOVED
- ❌ **aave_optimism_interests_test.sql**: Referenced deleted `aave_v3_optimism_interest_rates` - DELETED  
- File: `dbt_subprojects/hourly_spellbook/tests/_project/aave/optimism/aave_optimism_interests_test.sql` - REMOVED

## Compilation Status
✅ **dex**: Compiled successfully (1038 models, 411 seeds, 1903 data tests)
✅ **nft**: Compiled successfully (302 models, 681 data tests, 48 seeds)  
✅ **solana**: Compiled successfully (225 models, 328 data tests, 37 seeds)
✅ **tokens**: Compiled successfully (389 models, 508 data tests)
✅ **hourly_spellbook**: Compiled successfully (1360 models, 4734 data tests, 134 seeds) - FIXED
✅ **daily_spellbook**: Compiled successfully (1833 models, 2303 data tests, 47 seeds) - FIXED YAML syntax

## Issue Resolution
⚠️ **Restoration Required**: `lido_liquidity_zksync_maverick_pools.sql` 
- Issue: Model had dependent references in `lido_liquidity.sql`
- Resolution: Restored from main branch using `git checkout main -- <filepath>`
- Note: `git restore` did not work - needed checkout from main
- Note: `nexusmutual_ethereum_capital_pool_latest` had child dependencies

## Progress Tracking

### ✅ Completed Removals

- [x] **op_token_optimism.inflation_schedule** (daily_spellbook)
  - File: `dbt_subprojects/daily_spellbook/models/op/op_token/optimism/op_token_optimism_inflation_schedule.sql` - DELETED
  - Schema: `dbt_subprojects/daily_spellbook/models/op/op_token/optimism/op_token_optimism_schema.yml` (lines 55-81) - REMOVED

- [x] **aave_avalanche_c.interest_rates** (hourly_spellbook)
  - File: `dbt_subprojects/hourly_spellbook/models/_project/aave/avalanche_c/aave_avalanche_c_interest_rates.sql` - DELETED
  - Schema: `dbt_subprojects/hourly_spellbook/models/_project/aave/avalanche_c/_schema.yml` (lines 173-187) - REMOVED

- [x] **aave_bnb.interest_rates** (hourly_spellbook)
  - File: `dbt_subprojects/hourly_spellbook/models/_project/aave/bnb/aave_bnb_interest_rates.sql` - DELETED
  - Schema: `dbt_subprojects/hourly_spellbook/models/_project/aave/bnb/_schema.yml` (lines 173-187) - REMOVED

- [x] **aave_celo.interest_rates** (hourly_spellbook)
  - File: `dbt_subprojects/hourly_spellbook/models/_project/aave/celo/aave_celo_interest_rates.sql` - DELETED
  - Schema: `dbt_subprojects/hourly_spellbook/models/_project/aave/celo/_schema.yml` (lines 173-187) - REMOVED

- [x] **aave_fantom.interest_rates** (hourly_spellbook)
  - File: `dbt_subprojects/hourly_spellbook/models/_project/aave/fantom/aave_fantom_interest_rates.sql` - DELETED
  - Schema: `dbt_subprojects/hourly_spellbook/models/_project/aave/fantom/_schema.yml` (lines 173-187) - REMOVED

- [x] **aave_gnosis.interest_rates** (hourly_spellbook)
  - File: `dbt_subprojects/hourly_spellbook/models/_project/aave/gnosis/aave_gnosis_interest_rates.sql` - DELETED
  - Schema: `dbt_subprojects/hourly_spellbook/models/_project/aave/gnosis/_schema.yml` (lines 173-187) - REMOVED

- [x] **lido_liquidity_zksync.maverick_pools_zksync** (hourly_spellbook)
  - File: `dbt_subprojects/hourly_spellbook/models/_project/lido/liquidity/zksync/lido_liquidity_zksync_maverick_pools.sql` - DELETED
  - Schema: No schema entry found - NONE TO REMOVE

- [x] **nexusmutual_ethereum.capital_pool_latest** (daily_spellbook)
  - File: `dbt_subprojects/daily_spellbook/models/nexusmutual/ethereum/capital_pool/nexusmutual_ethereum_capital_pool_latest.sql` - DELETED
  - Schema: `dbt_subprojects/daily_spellbook/models/nexusmutual/ethereum/capital_pool/_schema.yml` (lines 190-230) - REMOVED

- [x] **no_schema.liquidity_manager_pools** (dex)
  - File: `dbt_subprojects/dex/models/_projects/uniswap/uniswap_liquidity_manager_pools.sql` - DELETED
  - Schema: `dbt_subprojects/dex/models/_projects/uniswap/_schema.yml` (lines 44-66) - REMOVED

- [x] **op_token_optimism.initial_allocations** (daily_spellbook)
  - File: `dbt_subprojects/daily_spellbook/models/op/op_token/optimism/op_token_optimism_initial_allocations.sql` - DELETED
  - Schema: `dbt_subprojects/daily_spellbook/models/op/op_token/optimism/op_token_optimism_schema.yml` (lines 33-53) - REMOVED

- [x] **op_token_distributions_optimism.foundation_wallet_approvals** (daily_spellbook)
  - File: `dbt_subprojects/daily_spellbook/models/op/token_distributions/optimism/op_token_distributions_optimism_foundation_wallet_approvals.sql` - DELETED
  - Schema: `dbt_subprojects/daily_spellbook/models/op/token_distributions/optimism/op_token_distributions_schema.yml` (lines 146-185) - REMOVED

- [x] **aave_linea.interest_rates** (hourly_spellbook)
  - File: `dbt_subprojects/hourly_spellbook/models/_project/aave/linea/aave_linea_interest_rates.sql` - DELETED
  - Schema: `dbt_subprojects/hourly_spellbook/models/_project/aave/linea/_schema.yml` (lines 198-217) - REMOVED

- [x] **aave_polygon.interest_rates** (hourly_spellbook)
  - File: `dbt_subprojects/hourly_spellbook/models/_project/aave/polygon/aave_polygon_interest_rates.sql` - DELETED
  - Schema: `dbt_subprojects/hourly_spellbook/models/_project/aave/polygon/_schema.yml` (lines 198-217) - REMOVED

- [x] **aave_scroll.interest_rates** (hourly_spellbook)
  - File: `dbt_subprojects/hourly_spellbook/models/_project/aave/scroll/aave_scroll_interest_rates.sql` - DELETED
  - Schema: `dbt_subprojects/hourly_spellbook/models/_project/aave/scroll/_schema.yml` (lines 198-217) - REMOVED

- [x] **aave_v2_ethereum.interest_rates** (hourly_spellbook)
  - File: `dbt_subprojects/hourly_spellbook/models/_project/aave/ethereum/aave_v2_ethereum_interest_rates.sql` - DELETED
  - Schema: No schema entry found - NONE TO REMOVE

- [x] **aave_v3_optimism.interest_rates** (hourly_spellbook)
  - File: `dbt_subprojects/hourly_spellbook/models/_project/aave/optimism/aave_v3_optimism_interest_rates.sql` - DELETED
  - Schema: `dbt_subprojects/hourly_spellbook/models/_project/aave/optimism/_schema.yml` (lines 218-238) - REMOVED

- [x] **aave_zksync.interest_rates** (hourly_spellbook)
  - File: `dbt_subprojects/hourly_spellbook/models/_project/aave/zksync/aave_zksync_interest_rates.sql` - DELETED
  - Schema: `dbt_subprojects/hourly_spellbook/models/_project/aave/zksync/_schema.yml` (lines 198-217) - REMOVED

- [x] **aztec_v2_ethereum.daily_bridge_activity** (daily_spellbook)
  - File: `dbt_subprojects/daily_spellbook/models/aztec/ethereum/aztec_v2_ethereum_daily_bridge_activity.sql` - DELETED
  - Schema: `dbt_subprojects/daily_spellbook/models/aztec/ethereum/aztec_ethereum_schema.yml` (lines 110-145) - REMOVED

- [x] **aztec_v2_ethereum.daily_estimated_rollup_tvl** (daily_spellbook)
  - File: `dbt_subprojects/daily_spellbook/models/aztec/ethereum/aztec_v2_ethereum_daily_estimated_rollup_tvl.sql` - DELETED
  - Schema: `dbt_subprojects/daily_spellbook/models/aztec/ethereum/aztec_ethereum_schema.yml` (lines 186-200) - REMOVED

- [x] **aztec_v2_ethereum.deposit_assets** (daily_spellbook)
  - File: `dbt_subprojects/daily_spellbook/models/aztec/ethereum/aztec_v2_ethereum_deposit_assets.sql` - DELETED
  - Schema: `dbt_subprojects/daily_spellbook/models/aztec/ethereum/aztec_ethereum_schema.yml` (lines 85-100) - REMOVED

- [x] **balances_polygon.erc20_hour** (daily_spellbook)
  - File: `dbt_subprojects/daily_spellbook/models/balances/polygon/erc20/balances_polygon_erc20_hour.sql` - DELETED
  - Schema: `dbt_subprojects/daily_spellbook/models/balances/polygon/erc20/balances_polygon_erc20_schema.yml` (lines 3-36) - REMOVED
  - Test: `dbt_subprojects/daily_spellbook/tests/balances/polygon/balances_polygon_erc20_hour_assert_nonnegative.sql` - DELETED

- [x] **balances_polygon.matic_hour** (daily_spellbook)
  - File: `dbt_subprojects/daily_spellbook/models/balances/polygon/matic/balances_polygon_matic_hour.sql` - DELETED
  - Schema: `dbt_subprojects/daily_spellbook/models/balances/polygon/matic/balances_polygon_matic_schema.yml` (lines 3-36) - REMOVED

- [x] **transfers_celo.erc721_rolling_hour** (daily_spellbook)
  - File: `dbt_subprojects/daily_spellbook/models/transfers/celo/erc721/transfers_celo_erc721_rolling_hour.sql` - DELETED
  - Schema: `dbt_subprojects/daily_spellbook/models/transfers/celo/erc721/transfers_celo_erc721_schema.yml` (lines 82-102) - REMOVED

- [x] **transfers_celo.erc721_rolling_day** (daily_spellbook)
  - File: `dbt_subprojects/daily_spellbook/models/transfers/celo/erc721/transfers_celo_erc721_rolling_day.sql` - DELETED
  - Schema: `dbt_subprojects/daily_spellbook/models/transfers/celo/erc721/transfers_celo_erc721_schema.yml` (lines 133-153) - REMOVED

- [x] **transfers_celo.erc1155_rolling_hour** (daily_spellbook)
  - File: `dbt_subprojects/daily_spellbook/models/transfers/celo/erc1155/transfers_celo_erc1155_rolling_hour.sql` - DELETED
  - Schema: `dbt_subprojects/daily_spellbook/models/transfers/celo/erc1155/transfers_celo_erc1155_schema.yml` (lines 83-103) - REMOVED

- [x] **transfers_celo.erc1155_rolling_day** (daily_spellbook)
  - File: `dbt_subprojects/daily_spellbook/models/transfers/celo/erc1155/transfers_celo_erc1155_rolling_day.sql` - DELETED
  - Schema: `dbt_subprojects/daily_spellbook/models/transfers/celo/erc1155/transfers_celo_erc1155_schema.yml` (lines 134-154) - REMOVED

- [x] **chainlink.chainlink_read_requests_feeds_daily** (daily_spellbook)
  - File: `dbt_subprojects/daily_spellbook/models/chainlink/chainlink_read_requests_feeds_daily.sql` - DELETED
  - Schema: `dbt_subprojects/daily_spellbook/models/chainlink/chainlink_schema.yml` (lines 1717-1750) - REMOVED

- [x] **chainlink.chainlink_read_requests_requester** (daily_spellbook)
  - File: `dbt_subprojects/daily_spellbook/models/chainlink/chainlink_read_requests_requester.sql` - DELETED
  - Schema: `dbt_subprojects/daily_spellbook/models/chainlink/chainlink_schema.yml` (lines 1870-1909) - REMOVED

- [x] **chainlink.chainlink_read_requests_requester_daily** (daily_spellbook)
  - File: `dbt_subprojects/daily_spellbook/models/chainlink/chainlink_read_requests_requester_daily.sql` - DELETED
  - Schema: `dbt_subprojects/daily_spellbook/models/chainlink/chainlink_schema.yml` (lines 1828-1869) - REMOVED

- [x] **tokemak_ethereum.tokemak_lookup_reactors** (daily_spellbook)
  - File: `dbt_subprojects/daily_spellbook/models/tokemak/ethereum/tokemak_ethereum_tokemak_lookup_reactors.sql` - DELETED
  - Schema: `dbt_subprojects/daily_spellbook/models/tokemak/ethereum/tokemak_ethereum_schema.yml` (lines 71-102) - REMOVED

- [x] **tokemak_ethereum.tokemak_addresses** (daily_spellbook)
  - File: `dbt_subprojects/daily_spellbook/models/tokemak/ethereum/tokemak_ethereum_tokemak_addresses.sql` - DELETED
  - Schema: `dbt_subprojects/daily_spellbook/models/tokemak/ethereum/tokemak_ethereum_schema.yml` (lines 45-70) - REMOVED

- [x] **tokemak_ethereum.lookup_tokens** (daily_spellbook)
  - File: `dbt_subprojects/daily_spellbook/models/tokemak/ethereum/tokemak_ethereum_lookup_tokens.sql` - DELETED
  - Schema: `dbt_subprojects/daily_spellbook/models/tokemak/ethereum/tokemak_ethereum_schema.yml` (lines 3-44) - REMOVED

- [x] **cow_protocol_gnosis.eth_flow_orders** (hourly_spellbook)
  - File: `dbt_subprojects/hourly_spellbook/models/_project/cow_protocol/gnosis/cow_protocol_gnosis_eth_flow_orders.sql` - DELETED
  - Schema: `dbt_subprojects/hourly_spellbook/models/_project/cow_protocol/gnosis/cow_protocol_gnosis_schema.yml` (lines 27-40) - REMOVED

- [x] **cryptopunks_ethereum.current_listings** (daily_spellbook)
  - File: `dbt_subprojects/daily_spellbook/models/cryptopunks/ethereum/cryptopunks_ethereum_current_listings.sql` - DELETED
  - Schema: `dbt_subprojects/daily_spellbook/models/cryptopunks/ethereum/cryptopunks_ethereum_schema.yml` (lines 19-36) - REMOVED

- [x] **cryptopunks_ethereum.floor_price_over_time** (daily_spellbook)
  - File: `dbt_subprojects/daily_spellbook/models/cryptopunks/ethereum/cryptopunks_ethereum_floor_price_over_time.sql` - DELETED
  - Schema: `dbt_subprojects/daily_spellbook/models/cryptopunks/ethereum/cryptopunks_ethereum_schema.yml` (lines 108-120) - REMOVED

- [x] **tessera_ethereum.bids** (daily_spellbook)
  - File: `dbt_subprojects/daily_spellbook/models/tessera/ethereum/tessera_ethereum_bids.sql` - DELETED
  - Schema: `dbt_subprojects/daily_spellbook/models/tessera/ethereum/tessera_ethereum_schema.yml` (lines 65-87) - REMOVED

- [x] **eigenlayer_ethereum.programmatic_incentive_by_day** (daily_spellbook)
  - File: `dbt_subprojects/daily_spellbook/models/_projects/eigenlayer/ethereum/eigenlayer_ethereum_programmatic_incentive_by_day.sql` - DELETED
  - Schema: `dbt_subprojects/daily_spellbook/models/_projects/eigenlayer/_schema.yml` (lines 199-210) - REMOVED

- [x] **sudoswap_ethereum.pool_balance_changes** (daily_spellbook)
  - File: Model file not found - may not exist in current codebase
  - Schema: No schema entry found - NONE TO REMOVE

- [x] **evms.erc1155_approvalsforall** (daily_spellbook)
  - File: `dbt_subprojects/daily_spellbook/models/evms/evms_tables/evms_erc1155_approvalsforall.sql` - DELETED
  - Schema: `dbt_subprojects/daily_spellbook/models/evms/evms_schema.yml` (lines 392-413) - REMOVED

- [x] **gmx_arbitrum.glp_aum** (daily_spellbook)
  - File: `dbt_subprojects/daily_spellbook/models/gmx/arbitrum/glp/gmx_arbitrum_glp_aum.sql` - DELETED
  - Schema: `dbt_subprojects/daily_spellbook/models/gmx/arbitrum/glp/gmx_arbitrum_glp_schema.yml` (lines 214-280) - REMOVED

- [x] **gmx_arbitrum.glp_fees** (daily_spellbook)
  - File: `dbt_subprojects/daily_spellbook/models/gmx/arbitrum/glp/gmx_arbitrum_glp_fees.sql` - DELETED
  - Schema: `dbt_subprojects/daily_spellbook/models/gmx/arbitrum/glp/gmx_arbitrum_glp_schema.yml` (lines 214-242) - REMOVED

- [x] **gmx_arbitrum.glp_float** (daily_spellbook)
  - File: `dbt_subprojects/daily_spellbook/models/gmx/arbitrum/glp/gmx_arbitrum_glp_float.sql` - DELETED
  - Schema: `dbt_subprojects/daily_spellbook/models/gmx/arbitrum/glp/gmx_arbitrum_glp_schema.yml` (lines 243-270) - REMOVED

- [x] **gmx_arbitrum.vault_balances** (daily_spellbook)
  - File: `dbt_subprojects/daily_spellbook/models/gmx/arbitrum/vault/gmx_arbitrum_vault_balances.sql` - DELETED
  - Schema: `dbt_subprojects/daily_spellbook/models/gmx/arbitrum/vault/gmx_arbitrum_vault_schema.yml` (entire file) - DELETED

- [x] **gmx_avalanche_c.glp_aum** (daily_spellbook)
  - File: `dbt_subprojects/daily_spellbook/models/gmx/avalanche_c/glp/gmx_avalanche_c_glp_aum.sql` - DELETED
  - Schema: `dbt_subprojects/daily_spellbook/models/gmx/avalanche_c/glp/gmx_avalanche_c_glp_schema.yml` (lines 157-210) - REMOVED
  - Test: `dbt_subprojects/daily_spellbook/tests/gmx/avalanche_c/gmx_avalanche_c_glp_aum_assert.sql` - DELETED

- [x] **gooddollar_celo.reserve_movement** (daily_spellbook)
  - File: `dbt_subprojects/daily_spellbook/models/gooddollar/celo/gooddollar_celo_reserve_movement.sql` - DELETED
  - Schema: `dbt_subprojects/daily_spellbook/models/gooddollar/celo/_schema.yml` (lines 225-264) - REMOVED

- [x] **gooddollar_celo.ubi_claimers_agg** (daily_spellbook)
  - File: `dbt_subprojects/daily_spellbook/models/gooddollar/celo/gooddollar_celo_ubi_claimers_agg.sql` - DELETED
  - Schema: `dbt_subprojects/daily_spellbook/models/gooddollar/celo/_schema.yml` (lines 111-130) - REMOVED

- [x] **keep3r_network.liquidity_addition** (daily_spellbook)
  - File: `dbt_subprojects/daily_spellbook/models/keep3r_network/keep3r_network_liquidity_addition.sql` - DELETED
  - Schema: `dbt_subprojects/daily_spellbook/models/keep3r_network/keep3r_network_schema.yml` (lines 4-23) - REMOVED

- [x] **keep3r_network.liquidity_withdrawal** (daily_spellbook)
  - File: `dbt_subprojects/daily_spellbook/models/keep3r_network/keep3r_network_liquidity_withdrawal.sql` - DELETED
  - Schema: `dbt_subprojects/daily_spellbook/models/keep3r_network/keep3r_network_schema.yml` (lines 24-43) - REMOVED

- [x] **labels.beraswap_pools_berachain** (daily_spellbook)
  - File: `dbt_subprojects/daily_spellbook/models/_sector/labels/addresses/__single_category_labels__/beraswap/labels_beraswap_pools_berachain.sql` - DELETED
  - Schema: `dbt_subprojects/daily_spellbook/models/_sector/labels/addresses/__single_category_labels__/beraswap/labels_beraswap_schema.yml` (entire file) - DELETED

- [x] **labels.burrbear_pools_berachain** (daily_spellbook)
  - File: `dbt_subprojects/daily_spellbook/models/_sector/labels/addresses/__single_category_labels__/burrbear/labels_burrbear_pools_berachain.sql` - DELETED
  - Schema: `dbt_subprojects/daily_spellbook/models/_sector/labels/addresses/__single_category_labels__/burrbear/labels_burrbear_schema.yml` (entire file) - DELETED

- [x] **labels.ofac_sanctioned_ethereum** (daily_spellbook) - **RESTORED** ⚠️
  - **Reason**: Referenced by `labels_addresses.sql` - has active dependencies, not eligible for removal
  - File: `dbt_subprojects/daily_spellbook/models/_sector/labels/addresses/__single_category_labels__/ofac_sanctionned/labels_ofac_sanctionned_ethereum.sql` - RESTORED
  - Schema: `dbt_subprojects/daily_spellbook/models/_sector/labels/addresses/__single_category_labels__/ofac_sanctionned/labels_ofac_sanctionned_ethereum_schema.yml` - RESTORED

- [x] **nomad_ethereum.view_bridge_transactions** (daily_spellbook)
  - File: `dbt_subprojects/daily_spellbook/models/nomad/ethereum/nomad_ethereum_view_bridge_transactions.sql` - DELETED
  - Schema: `dbt_subprojects/daily_spellbook/models/nomad/ethereum/nomad_ethereum_schema.yml` (entire model entry) - REMOVED

- [x] **labels.worldcoin_accounts** (daily_spellbook)
  - File: `dbt_subprojects/daily_spellbook/models/_sector/labels/addresses/__single_category_labels__/worldcoin/labels_worldcoin_accounts.sql` - DELETED
  - Schema: `dbt_subprojects/daily_spellbook/models/_sector/labels/addresses/__single_category_labels__/worldcoin/labels_worldcoin_accounts_schema.yml` - DELETED

- [x] **cryptopunks_ethereum.current_bids** (daily_spellbook)
  - File: `dbt_subprojects/daily_spellbook/models/cryptopunks/ethereum/cryptopunks_ethereum_current_bids.sql` - DELETED
  - Schema: `dbt_subprojects/daily_spellbook/models/cryptopunks/ethereum/cryptopunks_ethereum_schema.yml` (lines 144-167) - REMOVED

- [x] **balances_bnb.bnb_hour** (daily_spellbook)
  - File: `dbt_subprojects/daily_spellbook/models/balances/bnb/bnb/balances_bnb_bnb_hour.sql` - DELETED
  - Schema: `dbt_subprojects/daily_spellbook/models/balances/bnb/bnb/balances_bnb_bnb_schema.yml` (lines 3-36) - REMOVED

- [x] **aave_optimism.interest_rates** (hourly_spellbook)
  - File: `dbt_subprojects/hourly_spellbook/models/_project/aave/optimism/aave_optimism_interest_rates.sql` - DELETED
  - Schema: `dbt_subprojects/hourly_spellbook/models/_project/aave/optimism/_schema.yml` (lines 198-217) - REMOVED

- [x] **nft_ethereum.top_sales** (nft)
  - File: `dbt_subprojects/nft/models/nft_metrics/ethereum/nft_ethereum_top_sales.sql` - DELETED
  - Schema: `dbt_subprojects/nft/models/nft_metrics/ethereum/nft_ethereum_schema.yml` (lines 4-22) - REMOVED

- [x] **cryptopunks_ethereum.listings_over_time** (daily_spellbook)
  - File: `dbt_subprojects/daily_spellbook/models/cryptopunks/ethereum/cryptopunks_ethereum_listings_over_time.sql` - DELETED
  - Schema: `dbt_subprojects/daily_spellbook/models/cryptopunks/ethereum/cryptopunks_ethereum_schema.yml` (lines 75-85) - REMOVED

- [x] **zeroex_ethereum.nft_fills** (dex)
  - File: `dbt_subprojects/dex/models/_projects/zeroex/ethereum/zeroex_ethereum_nft_fills.sql` - DELETED
  - Schema: `dbt_subprojects/dex/models/_projects/zeroex/ethereum/zeroex_ethereum_schema.yml` (lines 153-207) - REMOVED

- [x] **evms.erc721_approvalsforall** (daily_spellbook)
  - File: `dbt_subprojects/daily_spellbook/models/evms/evms_tables/evms_erc721_approvalsforall.sql` - DELETED
  - Schema: No schema entry found - NONE TO REMOVE

- [x] **evms.erc721_approvals** (daily_spellbook)
  - File: `dbt_subprojects/daily_spellbook/models/evms/evms_tables/evms_erc721_approvals.sql` - DELETED
  - Schema: `dbt_subprojects/daily_spellbook/models/evms/evms_schema.yml` (lines 480-502) - REMOVED

- [x] **aave_sonic.interest_rates** (hourly_spellbook)
  - File: `dbt_subprojects/hourly_spellbook/models/_project/aave/sonic/aave_sonic_interest_rates.sql` - DELETED
  - Schema: `dbt_subprojects/hourly_spellbook/models/_project/aave/sonic/_schema.yml` (lines 198-217) - REMOVED

- [x] **aave_arbitrum.interest_rates** (hourly_spellbook)
  - File: `dbt_subprojects/hourly_spellbook/models/_project/aave/arbitrum/aave_arbitrum_interest_rates.sql` - DELETED
  - Schema: `dbt_subprojects/hourly_spellbook/models/_project/aave/arbitrum/_schema.yml` (lines 198-218) - REMOVED

- [x] **safe_berachain.bera_transfers** (hourly_spellbook)
  - File: `dbt_subprojects/hourly_spellbook/models/_project/safe/berachain/safe_berachain_bera_transfers.sql` - DELETED
  - Schema: `dbt_subprojects/hourly_spellbook/models/_project/safe/berachain/safe_berachain_schema.yml` (lines 36-75) - REMOVED

- [x] **cow_protocol.tx_hash_labels_all** (hourly_spellbook)
  - File: `dbt_subprojects/hourly_spellbook/models/_project/cow_protocol/tx_hash_labels/cow_protocol_tx_hash_labels_all.sql` - DELETED
  - Schema: `dbt_subprojects/hourly_spellbook/models/_project/cow_protocol/tx_hash_labels/cow_protocol_tx_hash_labels_all_schema.yml` (entire file) - DELETED

- [x] **nft_ethereum.top_minters** (nft)
  - File: `dbt_subprojects/nft/models/nft_metrics/ethereum/nft_ethereum_top_minters.sql` - DELETED
  - Schema: `dbt_subprojects/nft/models/nft_metrics/ethereum/nft_ethereum_schema.yml` (lines 4-26) - REMOVED

- [x] **gooddollar_celo.ubi_claims_daily_agg** (daily_spellbook)
  - File: `dbt_subprojects/daily_spellbook/models/gooddollar/celo/gooddollar_celo_ubi_claims_daily_agg.sql` - DELETED
  - Schema: `dbt_subprojects/daily_spellbook/models/gooddollar/celo/_schema.yml` (lines 77-110) - REMOVED

- [x] **keep3r_network.liquidity_credits_reward** (daily_spellbook)
  - File: `dbt_subprojects/daily_spellbook/models/keep3r_network/keep3r_network_liquidity_credits_reward.sql` - DELETED
  - Schema: `dbt_subprojects/daily_spellbook/models/keep3r_network/keep3r_network_schema.yml` (lines 63-86) - REMOVED

- [x] **keep3r_network.job_migration** (daily_spellbook)
  - File: `dbt_subprojects/daily_spellbook/models/keep3r_network/keep3r_network_job_migration.sql` - DELETED
  - Schema: `dbt_subprojects/daily_spellbook/models/keep3r_network/keep3r_network_schema.yml` (lines 4-20) - REMOVED

- [x] **chainlink.chainlink_read_requests_logs** (daily_spellbook)
  - File: `dbt_subprojects/daily_spellbook/models/chainlink/chainlink_read_requests_logs.sql` - DELETED
  - Schema: `dbt_subprojects/daily_spellbook/models/chainlink/chainlink_schema.yml` (lines 1749-1785) - REMOVED

- [x] **ens.view_registries** (hourly_spellbook)
  - File: `dbt_subprojects/hourly_spellbook/models/_project/ens/ens_view_registries.sql` - DELETED
  - Schema: `dbt_subprojects/hourly_spellbook/models/_project/ens/ens_ethereum_schema.yml` (lines 87-100) - REMOVED

- [x] **ens.view_renewals** (hourly_spellbook)
  - File: `dbt_subprojects/hourly_spellbook/models/_project/ens/ens_view_renewals.sql` - DELETED
  - Schema: `dbt_subprojects/hourly_spellbook/models/_project/ens/ens_ethereum_schema.yml` (lines 61-86) - REMOVED

- [x] **op_token_distributions_optimism.transfer_mapping** (daily_spellbook)
  - File: `dbt_subprojects/daily_spellbook/models/op/token_distributions/optimism/op_token_distributions_optimism_transfer_mapping.sql` - DELETED
  - Schema: `dbt_subprojects/daily_spellbook/models/op/token_distributions/optimism/op_token_distributions_schema.yml` (lines 3-85) - REMOVED

- [x] **cow_protocol_arbitrum.eth_flow_orders** (hourly_spellbook)
  - File: `dbt_subprojects/hourly_spellbook/models/_project/cow_protocol/arbitrum/cow_protocol_arbitrum_eth_flow_orders.sql` - DELETED
  - Schema: `dbt_subprojects/hourly_spellbook/models/_project/cow_protocol/arbitrum/cow_protocol_arbitrum_schema.yml` (lines 30-43) - REMOVED

- [x] **chainlink.chainlink_read_requests_feeds** (daily_spellbook)
  - File: `dbt_subprojects/daily_spellbook/models/chainlink/chainlink_read_requests_feeds.sql` - DELETED
  - Schema: `dbt_subprojects/daily_spellbook/models/chainlink/chainlink_schema.yml` (lines 1720-1742) - REMOVED

- [x] **addresses_ethereum.safe_airdrop** (daily_spellbook)
  - File: `dbt_subprojects/daily_spellbook/models/addresses/ethereum/addresses_ethereum_safe_airdrop.sql` - DELETED
  - Schema: No schema entry found - NONE TO REMOVE

- [x] **keep3r_network.keeper_work** (daily_spellbook)
  - File: `dbt_subprojects/daily_spellbook/models/keep3r_network/keep3r_network_keeper_work.sql` - DELETED
  - Schema: `dbt_subprojects/daily_spellbook/models/keep3r_network/keep3r_network_schema.yml` (lines 21-44) - REMOVED

- [x] **cow_protocol_base.eth_flow_orders** (hourly_spellbook)
  - File: `dbt_subprojects/hourly_spellbook/models/_project/cow_protocol/base/cow_protocol_base_eth_flow_orders.sql` - DELETED
  - Schema: `dbt_subprojects/hourly_spellbook/models/_project/cow_protocol/base/cow_protocol_base_schema.yml` (lines 30-43) - REMOVED

- [x] **dex.incentive_mappings** (daily_spellbook)
  - File: `dbt_subprojects/daily_spellbook/models/dex/dex_incentive_mappings.sql` - DELETED
  - Schema: `dbt_subprojects/daily_spellbook/models/dex/dex_schema.yml` (lines 3-42) - REMOVED

### 🔄 Currently Processing

*Ready for next batch...*

### ⏳ Pending Removals (0 references - highest priority)

*12 models remaining to be processed...*

## Rollback Commands

### Git Restore Commands
```bash
# To restore all deleted files (run from project root):
# git restore <file_path>
```

### Individual Rollback Log

1. **op_token_optimism.inflation_schedule** rollback:
   ```bash
   git restore dbt_subprojects/daily_spellbook/models/op/op_token/optimism/op_token_optimism_inflation_schedule.sql
   git restore dbt_subprojects/daily_spellbook/models/op/op_token/optimism/op_token_optimism_schema.yml
   ```

2. **aave_avalanche_c.interest_rates** rollback:
   ```bash
   git restore dbt_subprojects/hourly_spellbook/models/_project/aave/avalanche_c/aave_avalanche_c_interest_rates.sql
   git restore dbt_subprojects/hourly_spellbook/models/_project/aave/avalanche_c/_schema.yml
   ```

3. **aave_bnb.interest_rates** rollback:
   ```bash
   git restore dbt_subprojects/hourly_spellbook/models/_project/aave/bnb/aave_bnb_interest_rates.sql
   git restore dbt_subprojects/hourly_spellbook/models/_project/aave/bnb/_schema.yml
   ```

4. **aave_celo.interest_rates** rollback:
   ```bash
   git restore dbt_subprojects/hourly_spellbook/models/_project/aave/celo/aave_celo_interest_rates.sql
   git restore dbt_subprojects/hourly_spellbook/models/_project/aave/celo/_schema.yml
   ```

5. **aave_fantom.interest_rates** rollback:
   ```bash
   git restore dbt_subprojects/hourly_spellbook/models/_project/aave/fantom/aave_fantom_interest_rates.sql
   git restore dbt_subprojects/hourly_spellbook/models/_project/aave/fantom/_schema.yml
   ```

6. **aave_gnosis.interest_rates** rollback:
   ```bash
   git restore dbt_subprojects/hourly_spellbook/models/_project/aave/gnosis/aave_gnosis_interest_rates.sql
   git restore dbt_subprojects/hourly_spellbook/models/_project/aave/gnosis/_schema.yml
   ```

7. **lido_liquidity_zksync.maverick_pools_zksync** rollback:
   ```bash
   git restore dbt_subprojects/hourly_spellbook/models/_project/lido/liquidity/zksync/lido_liquidity_zksync_maverick_pools.sql
   ```

8. **nexusmutual_ethereum.capital_pool_latest** rollback:
   ```bash
   git restore dbt_subprojects/daily_spellbook/models/nexusmutual/ethereum/capital_pool/nexusmutual_ethereum_capital_pool_latest.sql
   git restore dbt_subprojects/daily_spellbook/models/nexusmutual/ethereum/capital_pool/_schema.yml
   ```

9. **no_schema.liquidity_manager_pools** rollback:
   ```bash
   git restore dbt_subprojects/dex/models/_projects/uniswap/uniswap_liquidity_manager_pools.sql
   git restore dbt_subprojects/dex/models/_projects/uniswap/_schema.yml
   ```

10. **op_token_optimism.initial_allocations** rollback:
    ```bash
    git restore dbt_subprojects/daily_spellbook/models/op/op_token/optimism/op_token_optimism_initial_allocations.sql
    git restore dbt_subprojects/daily_spellbook/models/op/op_token/optimism/op_token_optimism_schema.yml
    ```

11. **op_token_distributions_optimism.foundation_wallet_approvals** rollback:
    ```bash
    git restore dbt_subprojects/daily_spellbook/models/op/token_distributions/optimism/op_token_distributions_optimism_foundation_wallet_approvals.sql
    git restore dbt_subprojects/daily_spellbook/models/op/token_distributions/optimism/op_token_distributions_schema.yml
    ```

12. **aave_linea.interest_rates** rollback:
    ```bash
    git restore dbt_subprojects/hourly_spellbook/models/_project/aave/linea/aave_linea_interest_rates.sql
    git restore dbt_subprojects/hourly_spellbook/models/_project/aave/linea/_schema.yml
    ```

13. **aave_polygon.interest_rates** rollback:
    ```bash
    git restore dbt_subprojects/hourly_spellbook/models/_project/aave/polygon/aave_polygon_interest_rates.sql
    git restore dbt_subprojects/hourly_spellbook/models/_project/aave/polygon/_schema.yml
    ```

14. **aave_scroll.interest_rates** rollback:
    ```bash
    git restore dbt_subprojects/hourly_spellbook/models/_project/aave/scroll/aave_scroll_interest_rates.sql
    git restore dbt_subprojects/hourly_spellbook/models/_project/aave/scroll/_schema.yml
    ```

15. **aave_v2_ethereum.interest_rates** rollback:
    ```bash
    git restore dbt_subprojects/hourly_spellbook/models/_project/aave/ethereum/aave_v2_ethereum_interest_rates.sql
    ```

16. **aave_v3_optimism.interest_rates** rollback:
    ```bash
    git restore dbt_subprojects/hourly_spellbook/models/_project/aave/optimism/aave_v3_optimism_interest_rates.sql
    git restore dbt_subprojects/hourly_spellbook/models/_project/aave/optimism/_schema.yml
    ```

17. **aave_zksync.interest_rates** rollback:
    ```bash
    git restore dbt_subprojects/hourly_spellbook/models/_project/aave/zksync/aave_zksync_interest_rates.sql
    git restore dbt_subprojects/hourly_spellbook/models/_project/aave/zksync/_schema.yml
    ```

18. **aztec_v2_ethereum.daily_bridge_activity** rollback:
    ```bash
    git restore dbt_subprojects/daily_spellbook/models/aztec/ethereum/aztec_v2_ethereum_daily_bridge_activity.sql
    git restore dbt_subprojects/daily_spellbook/models/aztec/ethereum/aztec_ethereum_schema.yml
    ```

19. **aztec_v2_ethereum.daily_estimated_rollup_tvl** rollback:
    ```bash
    git restore dbt_subprojects/daily_spellbook/models/aztec/ethereum/aztec_v2_ethereum_daily_estimated_rollup_tvl.sql
    git restore dbt_subprojects/daily_spellbook/models/aztec/ethereum/aztec_ethereum_schema.yml
    ```

20. **aztec_v2_ethereum.deposit_assets** rollback:
    ```bash
    git restore dbt_subprojects/daily_spellbook/models/aztec/ethereum/aztec_v2_ethereum_deposit_assets.sql
    git restore dbt_subprojects/daily_spellbook/models/aztec/ethereum/aztec_ethereum_schema.yml
    ```

## Notes
- Processing 1 model at a time as requested
- Each model requires removal from both .sql file and schema.yml entry
- Keeping detailed rollback information for safety
- No dependencies to worry about - all models in list are dependency-free