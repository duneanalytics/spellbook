WITH lbps_call_create AS (
        SELECT * FROM {{ source('balancer_v2_ethereum', 'LiquidityBootstrappingPoolFactory_call_create') }}
        UNION ALL
        SELECT * FROM {{ source('balancer_v2_ethereum', 'NoProtocolFeeLiquidityBootstrappingPoolFactory_call_create') }}
    ),
    
    lbps_list AS (
        SELECT 
            posexplode(tokens) AS (token_index, token),
            lower(symbol) AS name,
            poolId AS pool_id,
            SUBSTRING(poolId, 0, 42) AS pool_address
        FROM {{ source('balancer_v2_ethereum', 'Vault_evt_PoolRegistered') }} c
        INNER JOIN lbps_call_create cc
        ON c.evt_tx_hash = cc.call_tx_hash
        AND cc.call_success
    ),
    
    lbps_weight_update AS (
        SELECT *
        FROM {{ source('balancer_v2_ethereum', 'LiquidityBootstrappingPool_evt_GradualWeightUpdateScheduled') }}
        UNION ALL
        SELECT *
        FROM {{ source('balancer_v2_ethereum', 'NoProtocolFeeLiquidityBootstrappingPool_evt_GradualWeightUpdateScheduled') }}
    ),
    
    last_weight_update AS (
        SELECT *
        FROM (
            SELECT 
                contract_address AS pool_address,
                from_unixtime(startTime, 'yyyy-MM-dd HH:mm') AS start_time,
                from_unixtime(endTime, 'yyyy-MM-dd HH:mm') AS end_time,
                startWeights AS start_weights,
                ROW_NUMBER() OVER (PARTITION BY contract_address ORDER BY evt_block_time DESC) AS ranking
            FROM lbps_weight_update c
        ) w
        WHERE ranking = 1
    ),
    
    last_weight_update_unnested AS (
        SELECT 
            pool_address,
            start_time,
            end_time,
            posexplode(start_weights) AS (start_weight_index, start_weight)
        FROM last_weight_update
    ),
    
    lbps_tokens_weights AS (
        SELECT 
            name,
            pool_id,
            l.pool_address,
            start_time,
            end_time,
            token,
            start_weight
        FROM lbps_list l
        LEFT JOIN last_weight_update_unnested w
        ON w.pool_address = l.pool_address
        AND w.start_weight_index = l.token_index
        ORDER BY w.pool_address
    ),
    
    lbps_info AS (
        SELECT 
            *
        FROM (
            SELECT 
                *,
               ROW_NUMBER() OVER (PARTITION BY pool_address ORDER BY start_weight DESC) AS ranking 
            FROM lbps_tokens_weights
            WHERE token NOT IN (
                '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2', -- WETH
                '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48', -- USDC
                '0x6b175474e89094c44da98b954eedeac495271d0f', -- DAI
                '0x88acdd2a6425c3faae4bc9650fd7e27e0bebb7ab', -- MIST
                '0x64aa3364f17a4d01c6f1751fd97c2bd3d7e7f1d5' -- OHM
            )
        ) l
        WHERE ranking = 1
    )
    
    SELECT 
        name,
        pool_id,
        token AS token_sold,
        t.symbol AS token_symbol,
        start_time,
        COALESCE(end_time, '2999-01-01') AS end_time
    FROM lbps_info l
    LEFT JOIN {{ ref('tokens_erc20') }} t
    ON l.token = t.contract_address
    ORDER BY pool_id
    