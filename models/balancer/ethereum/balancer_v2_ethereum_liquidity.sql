{{
    config(
        schema='balancer_v2_ethereum',
        alias = alias('liquidity'),
        tags = ['dunesql'],
        materialized = 'table',
        file_format = 'delta',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "balancer_v2",
                                    \'["stefenon", "viniabussafi"]\') }}'
    )
}}

WITH pool_labels AS (
    SELECT
        address AS pool_id,
        name AS pool_symbol
    FROM {{ ref('labels_balancer_v2_pools_ethereum') }}
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

    dex_prices_1 AS (
        SELECT
            date_trunc('day', HOUR) AS DAY,
            contract_address AS token,
            approx_percentile(median_price, 0.5) AS price,
            sum(sample_size) AS sample_size
        FROM {{ ref('dex_prices') }}
       GROUP BY 1, 2
        HAVING sum(sample_size) > 3
    ),
 
    dex_prices AS (
        SELECT
            *,
            LEAD(DAY, 1, NOW()) OVER (
                PARTITION BY token
                ORDER BY
                    DAY
            ) AS day_of_next_change
        FROM
            dex_prices_1
    ),

    bpt_prices AS(
        SELECT 
            date_trunc('day', HOUR) AS day,
            contract_address AS token,
            approx_percentile(median_price, 0.5) AS bpt_price
        FROM {{ ref('balancer_v2_ethereum_bpt_prices') }}
        GROUP BY 1, 2
    ),

    swaps_changes AS (
        SELECT
            day,
            pool_id,
            token,
            SUM(COALESCE(delta, INT256 '0')) AS delta
        FROM
            (
                SELECT
                    date_trunc('day', evt_block_time) AS day,
                    poolId AS pool_id,
                    tokenIn AS token,
                    CAST(amountIn as int256) AS delta
                FROM
                    {{ source('balancer_v2_ethereum', 'Vault_evt_Swap') }}
                UNION
                ALL
                SELECT
                    date_trunc('day', evt_block_time) AS day,
                    poolId AS pool_id,
                    tokenOut AS token,
                    -CAST(amountOut AS int256) AS delta
                FROM
                    {{ source('balancer_v2_ethereum', 'Vault_evt_Swap') }}
            ) swaps
        GROUP BY 1, 2, 3
    ),

    zipped_balance_changes AS (
        SELECT
            date_trunc('day', evt_block_time) AS day,
            poolId AS pool_id,
            t.tokens,
            d.deltas,
            p.protocolFeeAmounts
        FROM {{ source('balancer_v2_ethereum', 'Vault_evt_PoolBalanceChanged') }}
        CROSS JOIN UNNEST (tokens) WITH ORDINALITY as t(tokens,i)
        CROSS JOIN UNNEST (deltas) WITH ORDINALITY as d(deltas,i)
        CROSS JOIN UNNEST (protocolFeeAmounts) WITH ORDINALITY as p(protocolFeeAmounts,i)
        WHERE t.i = d.i AND d.i = p.i
        ORDER BY 1,2,3
    ),

    balances_changes AS (
        SELECT
            day,
            pool_id,
            tokens AS token,
            deltas - CAST(protocolFeeAmounts as int256) AS delta
        FROM zipped_balance_changes
        ORDER BY 1, 2, 3
    ),

    managed_changes AS (
        SELECT
            date_trunc('day', evt_block_time) AS day,
            poolId AS pool_id,
            token,
            cashDelta + managedDelta AS delta
        FROM {{ source('balancer_v2_ethereum', 'Vault_evt_PoolBalanceManaged') }}
    ),

    daily_delta_balance AS (
        SELECT
            day,
            pool_id,
            token,
            SUM(COALESCE(amount, INT256 '0')) AS amount
        FROM
            (
                SELECT
                    day,
                    pool_id,
                    token,
                    SUM(COALESCE(delta, INT256 '0')) AS amount
                FROM
                    balances_changes
                GROUP BY 1, 2, 3
                UNION ALL
                SELECT
                    day,
                    pool_id,
                    token,
                    delta AS amount
                FROM
                    swaps_changes
                UNION ALL
                SELECT
                    day,
                    pool_id,
                    token,
                    CAST(delta as int256) AS amount
                FROM
                    managed_changes
            ) balance
        GROUP BY 1, 2, 3
    ),

    cumulative_balance AS (
        SELECT
            DAY,
            pool_id,
            token,
            LEAD(DAY, 1, NOW()) OVER (PARTITION BY token, pool_id ORDER BY DAY) AS day_of_next_change,
            SUM(amount) OVER (PARTITION BY pool_id, token ORDER BY DAY ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_amount
        FROM daily_delta_balance
    ),

    calendar AS (
        SELECT date_sequence AS day
        FROM unnest(sequence(date('2021-04-21'), date(now()), interval '1' day)) as t(date_sequence)
    ),

   cumulative_usd_balance AS (
        SELECT
            c.day,
            b.pool_id,
            b.token,
            symbol AS token_symbol,
            cumulative_amount as token_balance_raw,
            cumulative_amount / POWER(10, COALESCE(t.decimals, p1.decimals)) AS token_balance,
            cumulative_amount / POWER(10, COALESCE(t.decimals, p1.decimals)) * COALESCE(p1.price, p2.price, 0) AS protocol_liquidity_usd,
            cumulative_amount / POWER(10, COALESCE(t.decimals, p1.decimals)) * COALESCE(p1.price, p2.price, p3.bpt_price) AS pool_liquidity_usd
        FROM calendar c
        LEFT JOIN cumulative_balance b ON b.day <= c.day
        AND c.day < b.day_of_next_change
        LEFT JOIN {{ ref('tokens_erc20') }} t ON t.contract_address = b.token
        AND blockchain = 'ethereum'
        LEFT JOIN prices p1 ON p1.day = b.day
        AND p1.token = b.token
        
        LEFT JOIN dex_prices p2 ON p2.day <= c.day
        AND c.day < p2.day_of_next_change
        AND p2.token = b.token
        
        LEFT JOIN bpt_prices p3 ON p3.day = b.day AND 
        p3.token = CAST(b.token as VARCHAR)
        WHERE b.token != BYTEARRAY_SUBSTRING(b.pool_id, 1, 20)
    ),

    pool_liquidity_estimates AS (
        SELECT
            b.day,
            b.pool_id,
            SUM(b.pool_liquidity_usd) / COALESCE(SUM(w.normalized_weight), 1) AS pool_liquidity
        FROM cumulative_usd_balance b
        LEFT JOIN {{ ref('balancer_v2_ethereum_pools_tokens_weights') }} w ON b.pool_id = w.pool_id
        AND b.token = w.token_address
        AND b.pool_liquidity_usd > 0
        GROUP BY 1, 2
    )

SELECT
    b.day,
    b.pool_id,
    p.pool_symbol,
    'ethereum' as blockchain,
    token AS token_address,
    token_symbol,
    token_balance_raw,
    token_balance,
    protocol_liquidity_usd,
    coalesce(pool_liquidity_usd, pool_liquidity * normalized_weight) AS pool_liquidity_usd
FROM pool_liquidity_estimates b
LEFT JOIN cumulative_usd_balance c ON c.day = b.day
AND c.pool_id = b.pool_id
LEFT JOIN {{ ref('balancer_v2_ethereum_pools_tokens_weights') }} w ON b.pool_id = w.pool_id
AND w.token_address = c.token
LEFT JOIN pool_labels p ON p.pool_id = BYTEARRAY_SUBSTRING(b.pool_id, 1, 20)

