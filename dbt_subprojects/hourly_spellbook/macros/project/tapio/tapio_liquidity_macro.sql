{% macro
    tapio_compatible_liquidity_macro(
        blockchain, 
        project, 
        version,
        factory_create_pool_function = null,
        factory_create_pool_evt = null,
        spa_minted_evt = null,
        spa_redeemed_evt = null,
        spa_swapped_evt = null,
        spa_donated_evt = null,
        start_date = "date('2025-01-01')"
    )
%}

WITH 
-- Pre-filter tokens table to reduce join complexity
relevant_tokens AS (
    SELECT
        contract_address,
        blockchain,
        symbol,
        decimals
    FROM {{ source('tokens', 'erc20') }}
    WHERE blockchain = '{{ blockchain }}'
),

-- Get pool addresses from pool creation events
pool_creation_events AS (
    SELECT
        evt_block_time,
        evt_tx_hash,
        selfPeggingAsset AS pool_address
    FROM {{ source(project ~ '_' ~ blockchain, factory_create_pool_evt) }}
),

-- Extract pool creation information from factory calls to get tokens
pool_creation_calls AS (
    SELECT
        call_tx_hash,
        call_block_time,
        CAST(from_hex(json_extract_scalar(argument, '$.tokenA')) AS varbinary) AS tokenA,
        CAST(from_hex(json_extract_scalar(argument, '$.tokenB')) AS varbinary) AS tokenB
    FROM {{ source(project ~ '_' ~ blockchain, factory_create_pool_function) }}
    WHERE call_success = true
),

-- Combine call and event data to map pool addresses to tokens
pool_tokens AS (
    SELECT
        e.pool_address,
        e.evt_block_time AS creation_time,
        c.tokenA AS token0,
        c.tokenB AS token1
    FROM pool_creation_events e
    JOIN pool_creation_calls c
        ON e.evt_tx_hash = c.call_tx_hash
),

-- Get swap events with proper direction handling
swap_events AS (
    SELECT
        buyer,
        contract_address,
        evt_block_number,
        evt_block_time,
        evt_tx_hash,
        evt_index,
        element_at(amounts, 1) AS amount0,
        element_at(amounts, 2) AS amount1,
        swapAmount,
        feeAmount
    FROM {{ source(project ~ '_' ~ blockchain, spa_swapped_evt) }}
    WHERE evt_block_date >= {{ start_date }}
),

-- Determine swap direction based on swap amount and amounts array
swap_direction AS (
    SELECT
        s.*,
        -- If swapAmount equals abs(amount0), then token0 is being sold (token0 = tokenIn)
        -- Otherwise, token1 is being sold (token1 = tokenIn)
        CASE
            WHEN ABS(amount0) = swapAmount THEN 0  -- token0 is being sold
            ELSE 1                                 -- token1 is being sold
        END AS tokenIn,
        CASE
            WHEN ABS(amount0) = swapAmount THEN 1  -- token1 is being bought
            ELSE 0                                 -- token0 is being bought
        END AS tokenOut,
        -- Determine the absolute amounts
        ABS(amount0) AS amount0_abs,
        ABS(amount1) AS amount1_abs
    FROM swap_events s
),

-- Get all liquidity events
all_liquidity_events AS (
    -- Swaps - use proper direction logic
    SELECT
        CAST(date_trunc('day', evt_block_time) AS DATE) AS day,
        contract_address AS pool_address,
        -- If token0 is being sold (tokenIn=0), pool loses token0 (negative), gains token1 (positive)
        -- If token1 is being sold (tokenIn=1), pool gains token0 (positive), loses token1 (negative)
        CASE
            WHEN tokenIn = 0 THEN -CAST(amount0_abs AS DECIMAL(38,0))
            ELSE CAST(amount0_abs AS DECIMAL(38,0))                  
        END AS amount0_delta,
        CASE
            WHEN tokenIn = 1 THEN -CAST(amount1_abs AS DECIMAL(38,0))
            ELSE CAST(amount1_abs AS DECIMAL(38,0))                  
        END AS amount1_delta
    FROM swap_direction

    UNION ALL
    -- Mints - tokens enter the pool (positive)
    SELECT
        CAST(date_trunc('day', evt_block_time) AS DATE) AS day,
        contract_address AS pool_address,
        CAST(element_at(amounts, 1) AS DECIMAL(38,0)) AS amount0_delta,
        CAST(element_at(amounts, 2) AS DECIMAL(38,0)) AS amount1_delta
    FROM {{ source(project ~ '_' ~ blockchain, spa_minted_evt) }}
    WHERE evt_block_date >= {{ start_date }}

    UNION ALL
    -- Redeems - tokens leave the pool (negative)
    SELECT
        CAST(date_trunc('day', evt_block_time) AS DATE) AS day,
        contract_address AS pool_address,
        -CAST(element_at(amounts, 1) AS DECIMAL(38,0)) AS amount0_delta,
        -CAST(element_at(amounts, 2) AS DECIMAL(38,0)) AS amount1_delta
    FROM {{ source(project ~ '_' ~ blockchain, spa_redeemed_evt) }}
    WHERE evt_block_date >= {{ start_date }}

    UNION ALL
    -- Donates - tokens enter the pool (positive)
    SELECT
        CAST(date_trunc('day', evt_block_time) AS DATE) AS day,
        contract_address AS pool_address,
        CAST(element_at(amounts, 1) AS DECIMAL(38,0)) AS amount0_delta,
        CAST(element_at(amounts, 2) AS DECIMAL(38,0)) AS amount1_delta 
    FROM {{ source(project ~ '_' ~ blockchain, spa_donated_evt) }}
    WHERE evt_block_date >= {{ start_date }}
),

-- Calculate daily token balances from liquidity events
daily_delta_balance AS (
    -- Token0 balances
    SELECT
        le.day,
        le.pool_address,
        p.token0 AS token_address,
        SUM(le.amount0_delta) AS amount
    FROM all_liquidity_events le
    JOIN pool_tokens p ON le.pool_address = p.pool_address
    GROUP BY 1, 2, 3
    
    UNION ALL
    
    -- Token1 balances
    SELECT
        le.day,
        le.pool_address,
        p.token1 AS token_address,
        SUM(le.amount1_delta) AS amount
    FROM all_liquidity_events le
    JOIN pool_tokens p ON le.pool_address = p.pool_address
    GROUP BY 1, 2, 3
),

-- Calculate cumulative balances over time
cumulative_balance AS (
    SELECT
        day,
        pool_address,
        token_address,
        LEAD(day, 1, CURRENT_DATE + INTERVAL '1' day) OVER (
            PARTITION BY token_address, pool_address 
            ORDER BY day
        ) AS day_of_next_change,
        SUM(amount) OVER (
            PARTITION BY pool_address, token_address 
            ORDER BY day 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cumulative_amount
    FROM daily_delta_balance
),

-- Join calendar with balances for complete daily series
daily_balances AS (
    SELECT
        c.timestamp as day,
        b.pool_address,
        b.token_address,
        b.cumulative_amount AS token_balance_raw
    FROM {{ source('utils', 'days') }} c
    LEFT JOIN cumulative_balance b 
        ON b.day <= c.timestamp AND c.timestamp < b.day_of_next_change
    WHERE b.pool_address IS NOT NULL
),

-- Get unique tokens used in pools to limit price queries
pool_tokens_list AS (
    SELECT DISTINCT token_address
    FROM daily_balances
    WHERE token_balance_raw IS NOT NULL
),

-- Get daily prices for tokens that are actually used in pools
daily_prices AS (
    SELECT
        CAST(date_trunc('day', minute) AS DATE) AS day,
        contract_address AS token,
        AVG(price) AS price,
        MAX(decimals) AS decimals
    FROM {{ source('prices', 'usd') }}
    WHERE blockchain = '{{ blockchain }}'
    AND contract_address IN (SELECT token_address FROM pool_tokens_list)
    AND minute >= {{ start_date }}
    GROUP BY 1, 2, decimals
),

-- Fill missing price days with forward-fill logic
prices_with_calendar AS (
    SELECT
        c.timestamp as day,
        pt.token_address,
        MAX_BY(dp.price, c.timestamp) OVER (
            PARTITION BY pt.token_address 
            ORDER BY c.timestamp 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS price,
        MAX_BY(dp.decimals, c.timestamp) OVER (
            PARTITION BY pt.token_address 
            ORDER BY c.timestamp 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS decimals
    FROM {{ source('utils', 'days') }} c
    CROSS JOIN pool_tokens_list pt
    LEFT JOIN daily_prices dp 
        ON c.timestamp = dp.day AND pt.token_address = dp.token
)

-- Final output with token details and USD values using daily prices
SELECT
    '{{ blockchain }}' AS blockchain,
    '{{ project }}' AS project,
    '{{ version }}' AS version,
    d.day,
    d.pool_address,
    d.token_address,
    t.symbol AS token_symbol,
    CONCAT(
        COALESCE(t0.symbol, 'UNKNOWN'), 
        '-', 
        COALESCE(t1.symbol, 'UNKNOWN')
    ) AS pool_name,
    d.token_balance_raw,
    pwc.price AS price,
    d.token_balance_raw / POWER(10, COALESCE(t.decimals, pwc.decimals, 18)) AS token_balance,
    (d.token_balance_raw / POWER(10, COALESCE(t.decimals, pwc.decimals, 18))) * COALESCE(pwc.price, 0) AS token_balance_usd
FROM daily_balances d
LEFT JOIN relevant_tokens t 
    ON t.contract_address = d.token_address
LEFT JOIN prices_with_calendar pwc 
    ON pwc.day = d.day AND pwc.token_address = d.token_address
LEFT JOIN pool_tokens pt ON d.pool_address = pt.pool_address
LEFT JOIN relevant_tokens t0 ON pt.token0 = t0.contract_address
LEFT JOIN relevant_tokens t1 ON pt.token1 = t1.contract_address
WHERE d.token_balance_raw IS NOT NULL
ORDER BY d.day, d.pool_address, d.token_address

{% endmacro %}