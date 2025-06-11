# Spellbook Model Cleanup Progress

## Summary
Removing 94 unused/lightly used models from the dbt project based on cleanup_candidates_20250610_163555.md

**Total models to remove:** 94
**Models processed:** 20
**Models remaining:** 74

## Subproject breakdown:
- **daily_spellbook**: 64 tables
- **hourly_spellbook**: 26 tables  
- **dex**: 2 tables
- **nft**: 2 tables

## Database Cleanup
✅ **SQL Drop Transaction Created**: `drop_unused_models.sql`
- Contains DROP statements for all 20 completed model deletions
- Uses transactions with IF EXISTS for safety
- Includes both TABLE and VIEW drop statements
- Ready for database execution

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
✅ **hourly_spellbook**: Compiled successfully (1361 models, 4734 data tests, 134 seeds) - FIXED
✅ **daily_spellbook**: Compiled successfully (1851 models, 2341 data tests, 47 seeds) - FIXED YAML syntax

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

### 🔄 Currently Processing

*Ready for next batch...*

### ⏳ Pending Removals (0 references - highest priority)

*74 models remaining to be processed...*

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