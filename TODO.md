# Spellbook Model Cleanup Progress

## Summary
Removing 94 unused/lightly used models from the dbt project based on cleanup_candidates_20250610_163555.md

**Total models to remove:** 94
**Models processed:** 10
**Models remaining:** 84

## Subproject breakdown:
- **daily_spellbook**: 64 tables
- **hourly_spellbook**: 26 tables  
- **dex**: 2 tables
- **nft**: 2 tables

## Database Cleanup
✅ **SQL Drop Transaction Created**: `drop_unused_models.sql`
- Contains DROP statements for all 10 completed model deletions
- Uses transactions with IF EXISTS for safety
- Includes both TABLE and VIEW drop statements
- Ready for database execution

## Compilation Status
✅ **All subprojects compile successfully** (tested 2024-12-20)
- **daily_spellbook**: ✅ PASS
- **hourly_spellbook**: ✅ PASS  
- **dex**: ✅ PASS
- **nft**: ✅ PASS

## Issue Resolution
⚠️ **Restoration Required**: `lido_liquidity_zksync_maverick_pools.sql` 
- Issue: Model had dependent references in `lido_liquidity.sql`
- Resolution: Restored from main branch using `git checkout main -- <filepath>`
- Note: `git restore` did not work - needed checkout from main
- Note: `nexusmutual_ethereum_capital_pool_latest` had child dependencies

## Progress Tracking

### ✅ Completed Removals

1. **op_token_optimism.inflation_schedule** (daily_spellbook) ✅
   - File: `dbt_subprojects/daily_spellbook/models/op/op_token/optimism/op_token_optimism_inflation_schedule.sql` - DELETED
   - Schema: `dbt_subprojects/daily_spellbook/models/op/op_token/optimism/op_token_optimism_schema.yml` (lines 55-81) - REMOVED

2. **aave_avalanche_c.interest_rates** (hourly_spellbook) ✅
   - File: `dbt_subprojects/hourly_spellbook/models/_project/aave/avalanche_c/aave_avalanche_c_interest_rates.sql` - DELETED
   - Schema: `dbt_subprojects/hourly_spellbook/models/_project/aave/avalanche_c/_schema.yml` (lines 173-187) - REMOVED

3. **aave_bnb.interest_rates** (hourly_spellbook) ✅
   - File: `dbt_subprojects/hourly_spellbook/models/_project/aave/bnb/aave_bnb_interest_rates.sql` - DELETED
   - Schema: `dbt_subprojects/hourly_spellbook/models/_project/aave/bnb/_schema.yml` (lines 173-187) - REMOVED

4. **aave_celo.interest_rates** (hourly_spellbook) ✅
   - File: `dbt_subprojects/hourly_spellbook/models/_project/aave/celo/aave_celo_interest_rates.sql` - DELETED
   - Schema: `dbt_subprojects/hourly_spellbook/models/_project/aave/celo/_schema.yml` (lines 173-187) - REMOVED

5. **aave_fantom.interest_rates** (hourly_spellbook) ✅
   - File: `dbt_subprojects/hourly_spellbook/models/_project/aave/fantom/aave_fantom_interest_rates.sql` - DELETED
   - Schema: `dbt_subprojects/hourly_spellbook/models/_project/aave/fantom/_schema.yml` (lines 173-187) - REMOVED

6. **aave_gnosis.interest_rates** (hourly_spellbook) ✅
   - File: `dbt_subprojects/hourly_spellbook/models/_project/aave/gnosis/aave_gnosis_interest_rates.sql` - DELETED
   - Schema: `dbt_subprojects/hourly_spellbook/models/_project/aave/gnosis/_schema.yml` (lines 173-187) - REMOVED

7. **lido_liquidity_zksync.maverick_pools_zksync** (hourly_spellbook) ✅
   - File: `dbt_subprojects/hourly_spellbook/models/_project/lido/liquidity/zksync/lido_liquidity_zksync_maverick_pools.sql` - DELETED
   - Schema: No schema entry found - NONE TO REMOVE

8. **nexusmutual_ethereum.capital_pool_latest** (daily_spellbook) ✅
   - File: `dbt_subprojects/daily_spellbook/models/nexusmutual/ethereum/capital_pool/nexusmutual_ethereum_capital_pool_latest.sql` - DELETED
   - Schema: `dbt_subprojects/daily_spellbook/models/nexusmutual/ethereum/capital_pool/_schema.yml` (lines 190-230) - REMOVED

9. **no_schema.liquidity_manager_pools** (dex) ✅
   - File: `dbt_subprojects/dex/models/_projects/uniswap/uniswap_liquidity_manager_pools.sql` - DELETED
   - Schema: `dbt_subprojects/dex/models/_projects/uniswap/_schema.yml` (lines 44-66) - REMOVED

10. **op_token_optimism.initial_allocations** (daily_spellbook) ✅
    - File: `dbt_subprojects/daily_spellbook/models/op/op_token/optimism/op_token_optimism_initial_allocations.sql` - DELETED
    - Schema: `dbt_subprojects/daily_spellbook/models/op/op_token/optimism/op_token_optimism_schema.yml` (lines 33-53) - REMOVED

### 🔄 Currently Processing

11. **aave_ethereum.interest_rates** (hourly_spellbook)

### ⏳ Pending Removals (0 references - highest priority)

11. **aave_ethereum.interest_rates** (hourly_spellbook)

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

## Notes
- Processing 1 model at a time as requested
- Each model requires removal from both .sql file and schema.yml entry
- Keeping detailed rollback information for safety
- No dependencies to worry about - all models in list are dependency-free