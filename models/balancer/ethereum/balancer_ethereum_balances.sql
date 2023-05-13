{{
    config(
        alias='balances',
        post_hook='{{ expose_spells_hide_trino(\'["ethereum"]\',
                                    "project",
                                    "balancer",
                                    \'["metacrypto", "jacektrocinski"]\') }}'
    )
}}

{% set balancer_contract = "0xba12222222228d8ba445958a75a0704d566bf2c8" %}

WITH pools AS (
    SELECT pool AS pools
    FROM {{ source('balancer_v1_ethereum', 'BFactory_evt_LOG_NEW_POOL') }}
),

joins AS (
    SELECT
        p.pools AS pool,
        DATE_TRUNC('day', e.evt_block_time) AS day,
        e.contract_address AS token,
        SUM(value) AS amount
    FROM {{ source('erc20_ethereum', 'evt_transfer') }} AS e
    INNER JOIN pools AS p ON e.`to` = p.pools
    GROUP BY 1, 2, 3
    UNION ALL
    SELECT
        e.`to` AS pool,
        DATE_TRUNC('day', e.evt_block_time) AS day,
        e.contract_address AS token,
        SUM(value) AS amount
    FROM {{ source('erc20_ethereum', 'evt_transfer') }} AS e
    WHERE e.`to` = '{{ balancer_contract }}'
    GROUP BY 1, 2, 3
),

exits AS (
    SELECT
        p.pools AS pool,
        DATE_TRUNC('day', e.evt_block_time) AS day,
        e.contract_address AS token,
        -SUM(value) AS amount
    FROM {{ source('erc20_ethereum', 'evt_transfer') }} AS e
    INNER JOIN pools AS p ON e.`from` = p.pools
    GROUP BY 1, 2, 3
    UNION ALL
    SELECT
        e.`from` AS pool,
        DATE_TRUNC('day', e.evt_block_time) AS day,
        e.contract_address AS token,
        -SUM(value) AS amount
    FROM {{ source('erc20_ethereum', 'evt_transfer') }} AS e
    WHERE e.`from` = '{{ balancer_contract }}'
    GROUP BY 1, 2, 3
),

daily_delta_balance_by_token AS (
    SELECT
        pool,
        day,
        token,
        SUM(COALESCE(amount, 0)) AS amount
    FROM
        (
            SELECT *
            FROM joins
            UNION ALL
            SELECT *
            FROM exits
        ) AS foo
    GROUP BY 1, 2, 3
),

cumulative_balance_by_token AS (
    SELECT
        pool,
        token,
        day,
        LEAD(day, 1, NOW()) OVER (PARTITION BY pool, token ORDER BY day) AS day_of_next_change,
        SUM(amount)
            OVER (PARTITION BY pool, token ORDER BY day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
        AS cumulative_amount
    FROM daily_delta_balance_by_token
),

calendar AS (
    SELECT EXPLODE(SEQUENCE(TO_DATE('2020-01-01'), CURRENT_DATE, INTERVAL 1 DAY)) AS day
),

running_cumulative_balance_by_token AS (
    SELECT
        c.day,
        pool,
        token,
        cumulative_amount
    FROM calendar AS c
    LEFT JOIN cumulative_balance_by_token AS b ON b.day <= c.day AND c.day < b.day_of_next_change
)

SELECT * FROM running_cumulative_balance_by_token;
