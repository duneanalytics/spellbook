{% set blockchain = 'ethereum' %}

{{
    config(
        schema='balancer_cowswap_amm_' + blockchain,
        alias = 'liquidity',
        materialized = 'table',
        file_format = 'delta'
    )
}}

WITH pool_labels AS (
    SELECT
        address,
        name
    FROM {{ source('labels', 'balancer_cowswap_amm_pools') }}
    WHERE blockchain = '{{blockchain}}'
    ),

    prices AS (
        SELECT
            date_trunc('day', minute) AS day,
            contract_address AS token,
            decimals,
            AVG(price) AS price
        FROM {{ source('prices', 'usd') }}
        WHERE blockchain = '{{blockchain}}'
        GROUP BY 1, 2, 3
    ),

    eth_prices AS(
        SELECT
            date_trunc('day', minute) AS day,
            AVG(price) AS eth_price
        FROM {{ source('prices', 'usd') }}
        WHERE blockchain = '{{blockchain}}'
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
        LEFT JOIN {{ source('tokens', 'erc20') }} t ON t.contract_address = b.token_address
        AND t.blockchain = '{{blockchain}}'
        LEFT JOIN prices p1 ON p1.day = b.day
        AND p1.token = b.token_address
    )
    
SELECT
    c.day,
    c.pool_address AS pool_id,
    c.pool_address,
    p.name AS pool_symbol,
    '1' AS version,
    '{{blockchain}}' AS blockchain,
    'balancer_cowswap_amm' AS pool_type,
    c.token_address,
    c.symbol AS token_symbol,
    c.token_balance_raw,
    c.token_balance,
    c.protocol_liquidity_usd,
    (c.protocol_liquidity_usd) / e.eth_price AS protocol_liquidity_eth,
    c.protocol_liquidity_usd AS pool_liquidity_usd,
    (c.protocol_liquidity_usd) / e.eth_price AS pool_liquidity_eth
FROM cumulative_usd_balance c
LEFT JOIN pool_labels p ON p.address = c.pool_address
LEFT JOIN eth_prices e ON e.day = c.day
WHERE c.pool_address IS NOT NULL