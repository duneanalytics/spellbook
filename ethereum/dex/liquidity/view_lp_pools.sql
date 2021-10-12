BEGIN;

DROP MATERIALIZED VIEW IF EXISTS dex.view_lp_pools;

CREATE MATERIALIZED VIEW dex.view_lp_pools AS (
    WITH balancer_v2_evts AS (
        SELECT
            SUBSTRING("poolId" FOR 20) as pool_address,
            unnest(tokens) AS token_address
        FROM balancer_v2."Vault_evt_PoolBalanceChanged"
        UNION ALL
        SELECT 
            SUBSTRING("poolId" FOR 20) as pool_address,
            "tokenIn"
        FROM balancer_v2."Vault_evt_Swap"
        UNION ALL
        SELECT  
            SUBSTRING("poolId" FOR 20) as pool_address,
            "tokenOut"
        FROM balancer_v2."Vault_evt_Swap"
        UNION ALL
        SELECT  
            SUBSTRING("poolId" FOR 20) as pool_address,
            token
        FROM balancer_v2."Vault_evt_PoolBalanceManaged"),
        distinct_pools AS (
        -- 1inch_v1
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
        -- balancer_v1
        SELECT 
            DISTINCT
            'Balancer' AS project,
            '1' AS version,
            contract_address as pool, 
            token
        FROM balancer."BPool_call_bind"
        WHERE call_success
        UNION ALL
        -- balancer_v2
        SELECT
            'Balancer' AS project,
            '2' AS version,
            pool_address,
            token_address
        FROM
        (
            SELECT
                DISTINCT
                pool_address,
                token_address 
                FROM balancer_v2_evts
        ) all_pools
        UNION ALL
        SELECT
            DISTINCT 
            'Bancor' AS project,
            '2' AS version,
            contract_address AS pool_address,
            CASE 
                WHEN "_reserveToken" = '\xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN '\x0000000000000000000000000000000000000000'::BYTEA
                ELSE "_reserveToken"
            END AS token 
        FROM bancor."StandardPoolConverter_evt_LiquidityAdded"
        UNION ALL
        -- Curve_v1
        SELECT
            DISTINCT
            'Curve' AS project,
            '1' AS version,
            exchange_contract_address AS pool,
            -- Add exception for curve v1 lp steth which contains `ETH`
            CASE WHEN exchange_contract_address = '\xdc24316b9ae028f1497c275eb9192a3ea0f67022' AND token_a_address = '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' 
                 THEN '\x0000000000000000000000000000000000000000'::BYTEA
                 ELSE token_a_address 
            END AS token
        FROM curvefi.view_trades
        UNION
        SELECT
            DISTINCT
            'Curve' AS project,
            '1' AS version,
            exchange_contract_address,
            -- Add exception for curve v1 lp steth which contains `ETH`
            CASE WHEN exchange_contract_address = '\xdc24316b9ae028f1497c275eb9192a3ea0f67022' AND token_b_address = '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' 
                 THEN '\x0000000000000000000000000000000000000000'::BYTEA
                 ELSE token_b_address 
            END 
        FROM curvefi.view_trades
        UNION ALL
        -- Sushiswap_v1
        SELECT
            'Sushiswap' AS project,
            '1' AS version,
            pair AS pool_address,
            token0 AS token_address
        FROM sushi."Factory_evt_PairCreated"
        UNION ALL
        SELECT
            'Sushiswap' AS project,
            '1' AS version,
            pair,
            token1
        FROM sushi."Factory_evt_PairCreated"
        UNION ALL
        -- Uniswap_v1
        -- ERC20 - ETH pairs
        SELECT
            'Uniswap' AS project,
            '1' AS version,
            exchange AS pool_address,
            token AS token_address
        FROM uniswap."Factory_evt_NewExchange"
        UNION ALL
        SELECT
            'Uniswap' AS project,
            '1' AS version,
            exchange,
            '\x0000000000000000000000000000000000000000'::BYTEA
        FROM uniswap."Factory_evt_NewExchange"
        UNION ALL
        -- Uniswap_v2
        -- ERC20 - ERC20 pairs
        SELECT
            'Uniswap' AS project,
            '2' AS version,
            pair AS pool_address,
            token0 AS token_address
        FROM uniswap_v2."Factory_evt_PairCreated"
        UNION ALL
        SELECT
            'Uniswap' AS project,
            '2' AS version,
            pair,
            token1 AS token_address
        FROM uniswap_v2."Factory_evt_PairCreated"
        UNION ALL
        -- Uniswap_v3
        SELECT
            'Uniswap' AS project,
            '3' AS version,
            pool AS pool_address,
            token0 AS token_address
        FROM uniswap_v3."Factory_evt_PoolCreated"
        UNION ALL
        SELECT
            'Uniswap' AS project,
            '3' AS version,
            pool,
            token1 AS token_address
        FROM uniswap_v3."Factory_evt_PoolCreated"
    )
    SELECT
        project,
        version,
        (labels.get(pool_address, 'lp_pool_name'))[1] AS pool_name,
        pool_address,
        CASE WHEN token_address = '\x0000000000000000000000000000000000000000'::BYTEA THEN 'ETH'
             WHEN token_address = '\xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee'::BYTEA THEN 'ETH'
             ELSE t.symbol
        END AS token_symbol,
        token_address,
        'CEX' AS category
    FROM distinct_pools p
    LEFT JOIN erc20.tokens t ON p.token_address = t.contract_address
);

CREATE UNIQUE INDEX IF NOT EXISTS dex_view_lp_pools_pool_addr_token_addr_uniq_idx ON dex.view_lp_pools (pool_address, token_address);
CREATE INDEX IF NOT EXISTS dex_view_lp_pools_pool_address_project_idx ON dex.view_lp_pools (pool_address, project);
CREATE INDEX IF NOT EXISTS dex_view_lp_pools_token_address_project_idx ON dex.view_lp_pools (token_address, project);
CREATE INDEX IF NOT EXISTS dex_view_lp_pools_token_symbol_idx ON dex.view_lp_pools (token_symbol);
CREATE INDEX IF NOT EXISTS dex_view_lp_pools_pool_address_idx ON dex.view_lp_pools (pool_address);
CREATE INDEX IF NOT EXISTS dex_view_lp_pools_token_address_idx ON dex.view_lp_pools (token_address);
CREATE INDEX IF NOT EXISTS dex_view_lp_pools_project_idx ON dex.view_lp_pools (project);

-- This script needs to run before (some of) the daily insert scripts into `dex.liquidity`: `dex.insert_liquidity_...` 
INSERT INTO cron.job(schedule, command)
VALUES ('1 7 * * *', $$REFRESH MATERIALIZED VIEW CONCURRENTLY dex.view_lp_pools$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;

COMMIT;
