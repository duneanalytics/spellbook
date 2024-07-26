{{
    config(
        schema = 'balancer_cowswap_amm_ethereum',
        alias = 'balances',
        materialized = 'table',
        file_format = 'delta'
    )
}}

WITH pools AS (
    SELECT 
        bPool AS pools
    FROM {{ source('b_cow_amm_ethereum', 'BCoWFactory_evt_LOG_NEW_POOL') }}
),

joins AS (
    SELECT 
        p.pools AS pool, 
        DATE_TRUNC('day', e.evt_block_time) AS day, 
        e.contract_address AS token, 
        SUM(CAST(value AS int256)) AS amount
    FROM {{ source('erc20_ethereum', 'evt_transfer') }} e
    INNER JOIN pools p ON e."to" = p.pools
    GROUP BY 1, 2, 3
),

exits AS (
    SELECT 
        p.pools AS pool, 
        DATE_TRUNC('day', e.evt_block_time) AS day, 
        e.contract_address AS token, 
        - SUM(CAST(value AS int256)) AS amount
    FROM {{ source('erc20_ethereum', 'evt_transfer') }} e
    INNER JOIN pools p ON e."from" = p.pools   
    GROUP BY 1, 2, 3
),

daily_delta_balance_by_token AS (
    SELECT 
        pool, 
        day, 
        token, 
        SUM(COALESCE(amount, CAST(0 AS int256))) AS amount 
    FROM 
        (SELECT *
        FROM joins j
        UNION ALL
        SELECT * 
        FROM exits e) foo
    GROUP BY 1, 2, 3
),

cumulative_balance_by_token AS (
    SELECT
        pool, 
        token, 
        day, 
        LEAD(day, 1, now()) OVER (PARTITION BY pool, token ORDER BY day) AS day_of_next_change,
        SUM(amount) OVER (PARTITION BY pool, token ORDER BY day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_amount
    FROM daily_delta_balance_by_token
),

calendar AS (
    SELECT 
        date_sequence AS day
    FROM unnest(sequence(date('2024-07-01'), date(now()), interval '1' day)) AS t(date_sequence)
)

SELECT
    c.day, 
    b.pool AS pool_address, 
    b.token AS token_address, 
    b.cumulative_amount AS token_balance_raw
FROM calendar c
LEFT JOIN cumulative_balance_by_token b ON b.day <= c.day AND c.day < b.day_of_next_change
WHERE b.pool IS NOT NULL
