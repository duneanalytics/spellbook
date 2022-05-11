DROP VIEW IF EXISTS balancer.view_vebal_balances;

CREATE VIEW balancer.view_vebal_balances AS

WITH base_locks AS (
        SELECT d.provider, ts AS locked_at, locktime AS unlocked_at, ts AS updated_at
        FROM balancer."veBAL_call_create_lock" l
        JOIN balancer."veBAL_evt_Deposit" d
        ON d.evt_tx_hash = l.call_tx_hash
        
        UNION ALL
        
        SELECT provider, null::numeric AS locked_at, locktime AS unlocked_at, ts AS updated_at
        FROM balancer."veBAL_evt_Deposit"
        WHERE value = 0
    ),
    
    decorated_locks AS (
        SELECT
          provider, unlocked_at, updated_at, FIRST_VALUE(locked_at) OVER (PARTITION BY provider, locked_partition ORDER BY updated_at) AS locked_at
        FROM (
          SELECT
            *,
            SUM(CASE WHEN locked_at IS NULL THEN 0 ELSE 1 END) OVER (PARTITION BY provider ORDER BY updated_at) AS locked_partition
          FROM base_locks
        ) AS foo
    ),
    
    locks_info AS (
        SELECT *, unlocked_at - locked_at AS lock_period
        FROM decorated_locks
    ),

    deposits AS (
        SELECT
            provider,
            date_trunc('day', evt_block_time) AS day,
            SUM(value/1e18) AS delta_bpt
        FROM balancer."veBAL_evt_Deposit"
        GROUP BY 1, 2
    ),
    
    withdrawals AS (
        SELECT 
            provider,
            date_trunc('day', evt_block_time) AS day,
            -SUM(value/1e18) AS delta_bpt
        FROM balancer."veBAL_evt_Withdraw"
        GROUP BY 1, 2
    ),
    
    bpt_locked_balance AS (
        SELECT day, provider, SUM(delta_bpt) AS bpt_balance
        FROM (
            SELECT * FROM deposits
            UNION ALL
            SELECT * FROM withdrawals
        ) union_all
        GROUP BY 1, 2
    ),
    
    calendar AS (
        SELECT generate_series(MIN(day), CURRENT_DATE, '1 day'::interval) AS day
        FROM bpt_locked_balance
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
    )
SELECT
    day,
    b.provider,
    bpt_balance,
    lock_period,
    COALESCE((bpt_balance *
    (lock_period / (365*86400)) *
    ((unlocked_at - (FLOOR(EXTRACT(EPOCH FROM b.day))+86400)) / lock_period)), 0) AS vebal
FROM running_balances b
LEFT JOIN locks_info l
ON l.provider = b.provider
AND l.updated_at = (
    SELECT MAX(updated_at)
    FROM locks_info
    WHERE updated_at <= (FLOOR(EXTRACT(EPOCH FROM b.day))+86400)
    AND provider = b.provider
)
;