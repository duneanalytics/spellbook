{% macro
    tapio_compatible_liquidity_macro(
        blockchain, 
        project, 
        version,
        factory_create_pool_function = null,
        factory_create_pool_evt = null,
        spa_token_swapped_evt = null
    )
%}

WITH prices AS (
    SELECT
        date_trunc('day', minute) AS day,
        contract_address AS token,
        decimals,
        AVG(price) AS price
    FROM {{ source('prices', 'usd') }}
    WHERE blockchain = {{ blockchain }}
    GROUP BY 1, 2, 3
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

-- Get swap events to track token movements
swap_events AS (
    SELECT
        date_trunc('day', evt_block_time) AS day,
        contract_address AS pool_address,
        element_at(amounts, 1) AS amount0_delta,
        element_at(amounts, 2) AS amount1_delta
    FROM {{ source(project ~ '_' ~ blockchain, spa_token_swapped_evt) }}
),

-- Calculate daily token balances from swaps
daily_delta_balance AS (
    SELECT
        s.day,
        s.pool_address,
        p.token0 AS token_address,
        SUM(s.amount0_delta) AS amount
    FROM swap_events s
    JOIN pool_tokens p ON s.pool_address = p.pool_address
    GROUP BY 1, 2, 3
    
    UNION ALL
    
    SELECT
        s.day,
        s.pool_address,
        p.token1 AS token_address,
        SUM(s.amount1_delta) AS amount
    FROM swap_events s
    JOIN pool_tokens p ON s.pool_address = p.pool_address
    GROUP BY 1, 2, 3
),

-- Calculate cumulative balances over time
cumulative_balance AS (
    SELECT
        day,
        pool_address,
        token_address,
        LEAD(day, 1, NOW()) OVER (
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

-- Generate a complete calendar for continuous data
calendar AS (
    SELECT date_sequence AS day
    FROM unnest(
        sequence(
            date('2023-01-01'), 
            date(now()), 
            interval '1' day
        )
    ) AS t(date_sequence)
),

-- Join calendar with balances for complete daily series
daily_balances AS (
    SELECT
        c.day,
        b.pool_address,
        b.token_address,
        b.cumulative_amount AS token_balance_raw
    FROM calendar c
    LEFT JOIN cumulative_balance b 
        ON b.day <= c.day AND c.day < b.day_of_next_change
    WHERE b.pool_address IS NOT NULL
)

-- Final output with token details and USD values
SELECT
    d.day,
    d.pool_address,
    d.token_address,
    t.symbol AS token_symbol,
    d.token_balance_raw,
    d.token_balance_raw / POWER(10, COALESCE(t.decimals, p.decimals)) AS token_balance,
    (d.token_balance_raw / POWER(10, COALESCE(t.decimals, p.decimals))) * COALESCE(p.price, 0) AS token_balance_usd,
    '{{ blockchain }}' AS blockchain,
    '{{ project }}' AS project,
    '{{ version }}' AS version
FROM daily_balances d
LEFT JOIN {{ source('tokens', 'erc20') }} t 
    ON t.contract_address = d.token_address
    AND t.blockchain = {{ blockchain }}
LEFT JOIN prices p 
    ON p.day = d.day
    AND p.token = d.token_address
WHERE d.token_balance_raw IS NOT NULL
{% endmacro %}