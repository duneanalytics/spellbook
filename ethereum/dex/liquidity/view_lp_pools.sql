BEGIN;

DROP MATERIALIZED VIEW IF EXISTS dex.view_lp_pools;

CREATE MATERIALIZED VIEW dex.view_lp_pools AS (
    WITH distinct_pools AS (
        -- 1inch v1
        SELECT
            '1inch' AS project,
            '1' AS version,
            mooniswap AS pool_address,
            token1 AS token_address
        FROM onelp."MooniswapFactory_evt_Deployed"
        UNION ALL
        SELECT
            '1inch' AS project,
            '1' AS version,
            mooniswap,
            token2
        FROM onelp."MooniswapFactory_evt_Deployed"
        UNION ALL
        SELECT
            '1inch' AS project,
            '1' AS version,
            mooniswap,
            token1
        FROM onelp."MooniswapFactory_v2_evt_Deployed"
        UNION ALL
        SELECT
            '1inch' AS project,
            '1' AS version,
            mooniswap,
            token2
        FROM onelp."MooniswapFactory_v2_evt_Deployed"
        UNION ALL
        -- balancer v1
        SELECT 
            DISTINCT -- is needed here :todo: at end
            'Balancer' AS project,
            '1' AS version,
            contract_address as pool, 
            token
        FROM balancer."BPool_call_bind"
        WHERE call_success
        -- balancer v2 :todo: review today
        UNION ALL
        SELECT
            'Balancer' AS project,
            '2' AS version,
            SUBSTRING(pool_id FOR 20) as pool_address,
            token_address
        FROM
        (
            select c."poolId" as pool_id, unnest(cc.tokens) as token_address, unnest(cc.weights)/1e18 as normalized_weight, cc.symbol, 'WP' as pool_type
            from balancer_v2."Vault_evt_PoolRegistered" c
            inner join balancer_v2."WeightedPoolFactory_call_create" cc
            on c.evt_tx_hash = cc.call_tx_hash
            union all
            select c."poolId" as pool_id, unnest(cc.tokens) as token_address, unnest(cc.weights)/1e18 as normalized_weight, cc.symbol, 'WP2T' as pool_type
            from balancer_v2."Vault_evt_PoolRegistered" c
            inner join balancer_v2."WeightedPool2TokensFactory_call_create" cc
            on c.evt_tx_hash = cc.call_tx_hash
        ) all_pools
        -- :todo: DO count at the end



        -- 1inch v1
        SELECT
            DISTINCT
            '1inch' AS project,
            '1' AS version,
            token1,
            token2,
            mooniswap
        FROM onelp."MooniswapFactory_evt_Deployed"
        UNION ALL
        SELECT
            DISTINCT
            '1inch' AS project,
            '1' AS version,
            token1,
            token2,
            mooniswap
        FROM onelp."MooniswapFactory_v2_evt_Deployed"
        -- 

        SELECT DISTINCT 
            pool_address,
            project,
            version,
            category
        FROM dex.liquidity
    )
    SELECT
        pool_address,
        (labels.get(pool_address, 'lp_pool_name'))[1] AS pool_name,
        project,
        version,
        category
    FROM distinct_pools
);

CREATE UNIQUE INDEX IF NOT EXISTS dex_view_lp_pools_pool_address_token1_token2_uniq_idx ON dex.view_lp_pools (pool_address, token1, token2);
CREATE INDEX IF NOT EXISTS dex_view_lp_pools_pool_address_project_idx ON dex.view_lp_pools (pool_address, project);
CREATE INDEX IF NOT EXISTS dex_view_lp_pools_pool_address_idx ON dex.view_lp_pools (pool_address);
CREATE INDEX IF NOT EXISTS dex_view_lp_pools_token1_idx ON dex.view_lp_pools (token1);
CREATE INDEX IF NOT EXISTS dex_view_lp_pools_token2_idx ON dex.view_lp_pools (token2);
CREATE INDEX IF NOT EXISTS dex_view_lp_pools_project_idx ON dex.view_lp_pools (project);

-- This script needs to run before (some of) the daily insert scripts into `dex.liquidity`: `dex.insert_liquidity_...` 
INSERT INTO cron.job(schedule, command)
VALUES ('1 7 * * *', $$REFRESH MATERIALIZED VIEW CONCURRENTLY dex.view_lp_pools$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;

COMMIT;
