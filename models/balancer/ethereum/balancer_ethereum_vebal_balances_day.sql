{{
    config(
        schema="balancer_ethereum",
        alias='vebal_balances_day',
        post_hook='{{ expose_spells_hide_trino(\'["ethereum"]\',
                                    "project",
                                    "balancer",
                                    \'["markusbkoch", "mendesfabio", "stefenon"]\') }}'
    )
}}

WITH base_locks AS (
    SELECT
        d.provider,
        ts AS locked_at,
        locktime AS unlocked_at,
        ts AS updated_at
    FROM {{ source('balancer_ethereum', 'veBAL_call_create_lock') }} AS l
    INNER JOIN {{ source('balancer_ethereum', 'veBAL_evt_Deposit') }} AS d
        ON d.evt_tx_hash = l.call_tx_hash

    UNION ALL

    SELECT
        provider,
        CAST(NULL AS NUMERIC(38)) AS locked_at,
        locktime AS unlocked_at,
        ts AS updated_at
    FROM {{ source('balancer_ethereum', 'veBAL_evt_Deposit') }}
    WHERE value = 0
),

decorated_locks AS (
    SELECT
        provider,
        unlocked_at,
        updated_at,
        FIRST_VALUE(locked_at) OVER (PARTITION BY provider, locked_partition ORDER BY updated_at) AS locked_at
    FROM (SELECT
        *,
        SUM(CASE WHEN locked_at IS NULL THEN 0 ELSE 1 END)
            OVER (PARTITION BY provider ORDER BY updated_at)
        AS locked_partition
    FROM base_locks) AS foo
),

locks_info AS (
    SELECT
        *,
        unlocked_at - locked_at AS lock_period
    FROM decorated_locks
),

deposits AS (
    SELECT
        provider,
        DATE_TRUNC('day', evt_block_time) AS day,
        SUM(value / 1e18) AS delta_bpt
    FROM {{ source('balancer_ethereum', 'veBAL_evt_Deposit') }}
    GROUP BY provider, day
),

withdrawals AS (
    SELECT
        provider,
        DATE_TRUNC('day', evt_block_time) AS day,
        -SUM(value / 1e18) AS delta_bpt
    FROM {{ source('balancer_ethereum', 'veBAL_evt_Withdraw') }}
    GROUP BY provider, day
),

bpt_locked_balance AS (
    SELECT
        day,
        provider,
        SUM(delta_bpt) AS bpt_balance
    FROM (
        SELECT * FROM deposits
        UNION ALL
        SELECT * FROM withdrawals
    ) AS union_all
    GROUP BY provider, day
),

calendar AS (
    SELECT EXPLODE(SEQUENCE(MIN(day), CURRENT_DATE, INTERVAL 1 DAY)) AS day
    FROM bpt_locked_balance
),

cumulative_balances AS (
    SELECT
        day,
        provider,
        LEAD(day, 1, NOW()) OVER (
            PARTITION BY provider
            ORDER BY day
        ) AS day_of_next_change,
        SUM(bpt_balance) OVER (
            PARTITION BY provider
            ORDER BY day
            ROWS BETWEEN
            UNBOUNDED PRECEDING AND
            CURRENT ROW
        ) AS bpt_balance
    FROM bpt_locked_balance
),

running_balances AS (
    SELECT
        c.day,
        provider,
        bpt_balance
    FROM calendar AS c
    LEFT JOIN cumulative_balances AS b
        ON
            b.day <= c.day
            AND c.day < b.day_of_next_change
),

double_counting AS (
    SELECT
        day,
        b.provider AS wallet_address,
        bpt_balance,
        updated_at,
        lock_period AS lock_time,
        GREATEST(
            COALESCE(
                bpt_balance
                * (lock_period / (365 * 86400))
                * ((unlocked_at - (UNIX_TIMESTAMP(b.day) + 86400)) / lock_period),
                0
            ),
            0
        ) AS vebal_balance
    FROM running_balances AS b
    LEFT JOIN locks_info AS l
        ON
            l.provider = b.provider
            AND l.updated_at <= UNIX_TIMESTAMP(b.day) + 86400
),

max_updated_at AS (
    SELECT
        day,
        wallet_address,
        MAX(updated_at) AS updated_at
    FROM double_counting
    GROUP BY day, wallet_address
)

SELECT
    a.day,
    a.wallet_address,
    a.bpt_balance,
    a.vebal_balance,
    a.lock_time
FROM double_counting AS a
INNER JOIN max_updated_at AS b
    ON
        a.day = b.day
        AND a.wallet_address = b.wallet_address
        AND a.updated_at = b.updated_at
