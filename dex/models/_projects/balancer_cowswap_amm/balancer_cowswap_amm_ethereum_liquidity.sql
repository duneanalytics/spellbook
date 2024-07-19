{{
    config(
        schema='balancer_cowswap_amm_ethereum',
        alias = 'liquidity',       
        materialized = 'table',
        file_format = 'delta'
    )
}}

WITH pool_labels AS (
    SELECT
        address,
        name
    FROM {{ ref('labels_balancer_cowswap_amm_pools_ethereum') }}
    ),

    prices AS (
        SELECT
            date_trunc('day', minute) AS day,
            contract_address AS token,
            decimals,
            AVG(price) AS price
        FROM {{ source('prices', 'usd') }}
        WHERE blockchain = 'ethereum'
        GROUP BY 1, 2, 3
    ),

    eth_prices AS(
        SELECT
            date_trunc('day', minute) AS day,
            AVG(price) AS eth_price
        FROM {{ source('prices', 'usd') }}
        WHERE blockchain = 'ethereum'
        AND symbol = 'ETH'
        GROUP BY 1
    ),

    cumulative_balance AS (
        SELECT
            day,
            pool_address,
            token_address,
            token_balance_raw
        FROM {{ ref('balancer_cowswap_amm_ethereum_balances') }} b
    ),
    
   cumulative_usd_balance AS (
        SELECT
            b.day,
            b.pool_address,
            b.token_address,
            t.symbol,
            token_balance_raw,
            token_balance_raw / POWER(10, COALESCE(t.decimals, p1.decimals)) AS token_balance,
            token_balance_raw / POWER(10, COALESCE(t.decimals, p1.decimals)) * COALESCE(p1.price, 0) AS protocol_liquidity_usd
        FROM cumulative_balance b
        LEFT JOIN {{ source('tokens_ethereum', 'erc20') }} t ON t.contract_address = b.token_address
        LEFT JOIN prices p1 ON p1.day = b.day
        AND p1.token = b.token_address
    )
    
        SELECT
            b.day,
            b.pool_address AS pool_id,
            b.pool_address,
            p.name AS pool_symbol,
            '1' AS version,
            'ethereum' AS blockchain,
            'balancer_cowswap_amm' AS pool_type,
            c.token_address,
            t.symbol AS token_symbol,
            token_balance_raw,
            token_balance,
            protocol_liquidity_usd,
            (protocol_liquidity_usd) / e.eth_price AS protocol_liquidity_eth
        FROM cumulative_usd_balance c
        LEFT JOIN {{ source('tokens_ethereum', 'erc20') }} t ON t.contract_address = c.token_address
        LEFT JOIN pool_labels p ON p.address = c.pool_address
        LEFT JOIN eth_prices e ON e.day = c.day
