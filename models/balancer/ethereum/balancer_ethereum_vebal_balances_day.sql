{{
    config(
        schema="balancer_ethereum",
        
        alias = 'vebal_balances_day',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "balancer",
                                    \'["markusbkoch", "mendesfabio", "stefenon", "viniabussafi"]\') }}'
    )
}}

WITH base_locks AS (
        SELECT d.provider, ts AS locked_at, locktime AS unlocked_at, ts AS updated_at
        FROM {{ source('balancer_ethereum', 'veBAL_call_create_lock') }} l  
        JOIN {{ source('balancer_ethereum', 'veBAL_evt_Deposit') }} d
        ON d.evt_tx_hash = l.call_tx_hash
        
        UNION ALL
        
        SELECT provider, CAST(null as UINT256) AS locked_at, locktime AS unlocked_at, ts AS updated_at
        FROM {{ source('balancer_ethereum', 'veBAL_evt_Deposit') }}
        WHERE CAST(value AS DOUBLE) = 0
    ),
    
    decorated_locks AS (
        SELECT provider,
               unlocked_at,
               updated_at,
               FIRST_VALUE(locked_at) OVER (PARTITION BY provider, locked_partition ORDER BY updated_at) AS locked_at
        FROM (SELECT *,
                     SUM(CASE WHEN locked_at IS NULL THEN 0 ELSE 1 END)
                         OVER (PARTITION BY provider ORDER BY updated_at) AS locked_partition
              FROM base_locks) AS foo
    ),
    
    locks_info AS (
        SELECT *, unlocked_at - locked_at AS lock_period
        FROM decorated_locks
    ),

    deposits AS (
        SELECT
            provider,
            date_trunc('day', evt_block_time) AS day,
            SUM(CAST(value AS DOUBLE))/1e18 AS delta_bpt
        FROM {{ source('balancer_ethereum', 'veBAL_evt_Deposit') }}
        GROUP BY 1, 2
    ),
    
    withdrawals AS (
        SELECT 
            provider,
            date_trunc('day', evt_block_time) AS day,
            -SUM(CAST(value AS DOUBLE))/1e18 AS delta_bpt
        FROM {{ source('balancer_ethereum', 'veBAL_evt_Withdraw') }}
        GROUP BY 1, 2
    ),
    
    bpt_locked_balance AS (
        SELECT day, provider, SUM(delta_bpt) AS bpt_balance
        FROM (
            SELECT * FROM deposits
            UNION ALL
            SELECT * FROM withdrawals
        ) union_all
        GROUP BY 2, 1
    ),
    
    days_seq AS (
        SELECT sequence(CAST(MIN(day) AS TIMESTAMP), CAST(now() AS TIMESTAMP), interval '1' day) as day
        FROM bpt_locked_balance
    ),

    calendar AS (
        SELECT days.day FROM days_seq CROSS JOIN unnest(day) as days(day)
    ),
    
    cumulative_balances AS (
        SELECT
            day,
            provider,
            LEAD(DAY, 1, NOW()) OVER (
                PARTITION BY provider
                ORDER BY day
            ) AS day_of_next_change,
            SUM(bpt_balance) OVER (
                PARTITION BY provider
                ORDER BY DAY
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
        FROM calendar c
        LEFT JOIN cumulative_balances b
        ON b.day <= c.day
        AND c.day < b.day_of_next_change
    ),
    
    double_counting AS (
        SELECT
            day,
            b.provider as wallet_address,
            bpt_balance,
            updated_at,
            lock_period as lock_time,
            GREATEST(
                COALESCE(
                    bpt_balance *
                    (CAST(lock_period AS DOUBLE) / CAST(365*86400 AS DOUBLE)) *
                    ((CAST(unlocked_at AS DOUBLE) - (to_unixtime(b.day)+86400)) / CAST(lock_period AS DOUBLE))
                    , 0),
                0
             ) AS vebal_balance
        FROM running_balances b
        LEFT JOIN locks_info l
        ON l.provider = b.provider
        AND l.updated_at <= CAST(to_unixtime(b.day) + 86400 AS UINT256)
    ),
    
    max_updated_at AS (
        SELECT 
            day,
            wallet_address,
            max(updated_at) AS updated_at
        FROM double_counting
        GROUP BY day, wallet_address
    )
    
SELECT 
    a.day,
    a.wallet_address,
    a.bpt_balance,
    a.vebal_balance,
    a.lock_time
FROM double_counting a
INNER JOIN max_updated_at b
ON a.day = b.day
AND a.wallet_address = b.wallet_address
AND a.updated_at = b.updated_at
