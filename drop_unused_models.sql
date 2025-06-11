-- Transaction to remove unused database tables/views
-- Based on completed model deletions from Spellbook Model Cleanup
-- Generated on: 2024-12-20

BEGIN TRANSACTION;

-- Drop tables/views for completed model deletions
-- Using IF EXISTS to avoid errors if tables don't exist

-- 1. op_token_optimism.inflation_schedule
DROP TABLE IF EXISTS op_token_optimism.inflation_schedule;
DROP VIEW IF EXISTS op_token_optimism.inflation_schedule;

-- 2. aave_avalanche_c.interest_rates  
DROP TABLE IF EXISTS aave_avalanche_c.interest_rates;
DROP VIEW IF EXISTS aave_avalanche_c.interest_rates;

-- 3. aave_bnb.interest_rates
DROP TABLE IF EXISTS aave_bnb.interest_rates;
DROP VIEW IF EXISTS aave_bnb.interest_rates;

-- 4. aave_celo.interest_rates
DROP TABLE IF EXISTS aave_celo.interest_rates;
DROP VIEW IF EXISTS aave_celo.interest_rates;

-- 5. aave_fantom.interest_rates
DROP TABLE IF EXISTS aave_fantom.interest_rates;
DROP VIEW IF EXISTS aave_fantom.interest_rates;

-- 6. aave_gnosis.interest_rates
DROP TABLE IF EXISTS aave_gnosis.interest_rates;
DROP VIEW IF EXISTS aave_gnosis.interest_rates;

-- 7. lido_liquidity_zksync.maverick_pools_zksync
DROP TABLE IF EXISTS lido_liquidity_zksync.maverick_pools_zksync;
DROP VIEW IF EXISTS lido_liquidity_zksync.maverick_pools_zksync;

-- 8. nexusmutual_ethereum.capital_pool_latest
DROP TABLE IF EXISTS nexusmutual_ethereum.capital_pool_latest;
DROP VIEW IF EXISTS nexusmutual_ethereum.capital_pool_latest;

-- 9. no_schema.liquidity_manager_pools (note: this may need schema adjustment)
DROP TABLE IF EXISTS liquidity_manager_pools;
DROP VIEW IF EXISTS liquidity_manager_pools;

-- 10. op_token_optimism.initial_allocations
DROP TABLE IF EXISTS op_token_optimism.initial_allocations;
DROP VIEW IF EXISTS op_token_optimism.initial_allocations;

-- 11. op_token_distributions_optimism.foundation_wallet_approvals
DROP TABLE IF EXISTS op_token_distributions_optimism.foundation_wallet_approvals;
DROP VIEW IF EXISTS op_token_distributions_optimism.foundation_wallet_approvals;

-- 12. aave_linea.interest_rates
DROP TABLE IF EXISTS aave_linea.interest_rates;
DROP VIEW IF EXISTS aave_linea.interest_rates;

-- 13. aave_polygon.interest_rates
DROP TABLE IF EXISTS aave_polygon.interest_rates;
DROP VIEW IF EXISTS aave_polygon.interest_rates;

-- 14. aave_scroll.interest_rates
DROP TABLE IF EXISTS aave_scroll.interest_rates;
DROP VIEW IF EXISTS aave_scroll.interest_rates;

-- 15. aave_v2_ethereum.interest_rates
DROP TABLE IF EXISTS aave_v2_ethereum.interest_rates;
DROP VIEW IF EXISTS aave_v2_ethereum.interest_rates;

-- 16. aave_v3_optimism.interest_rates
DROP TABLE IF EXISTS aave_v3_optimism.interest_rates;
DROP VIEW IF EXISTS aave_v3_optimism.interest_rates;

-- 17. aave_zksync.interest_rates
DROP TABLE IF EXISTS aave_zksync.interest_rates;
DROP VIEW IF EXISTS aave_zksync.interest_rates;

-- 18. aztec_v2_ethereum.daily_bridge_activity
DROP TABLE IF EXISTS aztec_v2_ethereum.daily_bridge_activity;
DROP VIEW IF EXISTS aztec_v2_ethereum.daily_bridge_activity;

-- 19. aztec_v2_ethereum.daily_estimated_rollup_tvl
DROP TABLE IF EXISTS aztec_v2_ethereum.daily_estimated_rollup_tvl;
DROP VIEW IF EXISTS aztec_v2_ethereum.daily_estimated_rollup_tvl;

-- 20. aztec_v2_ethereum.deposit_assets
DROP TABLE IF EXISTS aztec_v2_ethereum.deposit_assets;
DROP VIEW IF EXISTS aztec_v2_ethereum.deposit_assets;

COMMIT TRANSACTION;

-- Rollback command if needed:
-- ROLLBACK TRANSACTION; 