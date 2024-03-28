{{
    config(
        schema='beethoven_x_fantom',
        alias = 'liquidity',
        materialized = 'table',
        file_format = 'delta',
        post_hook='{{ expose_spells(\'["fantom"]\',
                        "project",
                        "beethoven_x",
                        \'["viniabussafi"]\') }}'
    )
}}

WITH pool_labels AS (
        SELECT
            address AS pool_id,
            name AS pool_symbol,
            pool_type
        FROM {{ ref('labels_beethoven_x_pools_fantom') }}
        WHERE blockchain = 'fantom'
    ),

    prices AS (
        SELECT
            date_trunc('day', minute) AS day,
            contract_address AS token,
            decimals,
            AVG(price) AS price
        FROM {{ source('prices', 'usd') }}
        WHERE blockchain = 'fantom'
        GROUP BY 1, 2, 3
    ),
    
    eth_prices AS (
        SELECT 
            DATE_TRUNC('day', minute) as day,
            AVG(price) as eth_price
        FROM {{ source('prices', 'usd') }}
        WHERE symbol = 'ETH'
        GROUP BY 1
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
                FROM {{ source('beethoven_x_fantom', 'Vault_evt_Swap') }}

                UNION ALL

                SELECT
                    date_trunc('day', evt_block_time) AS day,
                    poolId AS pool_id,
                    tokenOut AS token,
                    -CAST(amountOut AS int256) AS delta
                FROM {{ source('beethoven_x_fantom', 'Vault_evt_Swap') }}
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
        FROM {{ source('beethoven_x_fantom', 'Vault_evt_PoolBalanceChanged') }}
        CROSS JOIN UNNEST (tokens) WITH ORDINALITY as t(tokens,i)
        CROSS JOIN UNNEST (deltas) WITH ORDINALITY as d(deltas,i)
        CROSS JOIN UNNEST (protocolFeeAmounts) WITH ORDINALITY as p(protocolFeeAmounts,i)
        WHERE t.i = d.i 
        AND d.i = p.i
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
        FROM {{ source('beethoven_x_fantom', 'Vault_evt_PoolBalanceManaged') }}
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
                FROM balances_changes
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
                    CAST(delta AS int256) AS amount
                FROM managed_changes
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
            'fantom' as blockchain,
            b.pool_id,
            b.token,
            symbol AS token_symbol,
            cumulative_amount as token_balance_raw,
            cumulative_amount / POWER(10, COALESCE(t.decimals, p1.decimals)) AS token_balance,
            cumulative_amount / POWER(10, COALESCE(t.decimals, p1.decimals)) * COALESCE(p1.price, 0) AS protocol_liquidity_usd
        FROM calendar c
        LEFT JOIN cumulative_balance b ON b.day <= c.day
        AND c.day < b.day_of_next_change
        LEFT JOIN {{ source('tokens', 'erc20') }} t ON t.contract_address = b.token
        AND blockchain = 'fantom'
        LEFT JOIN prices p1 ON p1.day = b.day
        AND p1.token = b.token
        WHERE b.token != BYTEARRAY_SUBSTRING(b.pool_id, 1, 20)
    ),

    weighted_pool_liquidity_estimates AS (
        SELECT
            b.day,
            b.pool_id,
            q.name,
            pool_type,
            ROW_NUMBER() OVER (partition by b.day, b.pool_id ORDER BY SUM(b.protocol_liquidity_usd) ASC) AS pricing_count, --to avoid double count in pools with multiple pricing assets
            SUM(b.protocol_liquidity_usd) / COALESCE(SUM(w.normalized_weight), 1) AS protocol_liquidity
        FROM cumulative_usd_balance b
        LEFT JOIN {{ ref('beethoven_x_fantom_pools_tokens_weights') }} w ON b.pool_id = w.pool_id 
        AND b.token = w.token_address
        AND b.protocol_liquidity_usd > 0
        LEFT JOIN {{ ref('balancer_token_whitelist') }} q ON b.token = q.address 
        AND b.blockchain = q.chain
        LEFT JOIN pool_labels p ON p.pool_id = BYTEARRAY_SUBSTRING(b.pool_id, 1, 20)
        WHERE q.name IS NOT NULL 
        AND p.pool_type IN ('weighted') -- filters for weighted pools with pricing assets
        AND w.blockchain = 'fantom'
        GROUP BY 1, 2, 3, 4
    ),
    
    weighted_pool_liquidity_estimates_2 AS(
    SELECT  e.day,
            e.pool_id,
            SUM(e.protocol_liquidity) / MAX(e.pricing_count) AS protocol_liquidity
    FROM weighted_pool_liquidity_estimates e
    GROUP BY 1,2
    )
    
    SELECT
        c.day,
        c.pool_id,
        BYTEARRAY_SUBSTRING(c.pool_id, 1, 20) AS pool_address,
        p.pool_symbol,
        'fantom' AS blockchain,
        c.token AS token_address,
        c.token_symbol,
        c.token_balance_raw,
        c.token_balance,
        COALESCE(b.protocol_liquidity * w.normalized_weight, c.protocol_liquidity_usd) AS protocol_liquidity_usd,
        COALESCE(b.protocol_liquidity * w.normalized_weight, c.protocol_liquidity_usd)/e.eth_price AS protocol_liquidity_eth
    FROM cumulative_usd_balance c
    FULL OUTER JOIN weighted_pool_liquidity_estimates_2 b ON c.day = b.day
    AND c.pool_id = b.pool_id
    LEFT JOIN {{ ref('beethoven_x_fantom_pools_tokens_weights') }} w ON b.pool_id = w.pool_id 
    AND w.blockchain = 'fantom'
    AND w.token_address = c.token
    LEFT JOIN eth_prices e ON e.day = c.day 
    LEFT JOIN pool_labels p ON p.pool_id = BYTEARRAY_SUBSTRING(c.pool_id, 1, 20)
