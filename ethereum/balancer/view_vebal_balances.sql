CREATE OR REPLACE VIEW balancer.view_vebal_balances AS

WITH locks_info AS (
        SELECT
            provider,
            MAX(locktime) - MIN(ts) AS lock_period, 
            MIN(ts) AS locked_at,
            MAX(locktime) AS unlocked_at
        FROM balancer."veBAL_evt_Deposit"
        GROUP BY 1
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
    
    cumulative_bpt_balance AS (
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
    
    running_bpt_balance AS (
        SELECT
            c.day,
            provider,
            bpt_balance
        FROM calendar c
        LEFT JOIN cumulative_bpt_balance b
        ON b.day <= c.day
        AND c.day < b.day_of_next_change
    )

SELECT
    day,
    FLOOR(EXTRACT(EPOCH FROM day)) AS day_ts,
    b.provider,
    bpt_balance,
    bpt_balance *
    (lock_period / (365*86400)) *
    ((unlocked_at - FLOOR(EXTRACT(EPOCH FROM day))) / lock_period) AS vebal
FROM running_bpt_balance b
LEFT JOIN locks_info l
ON b.provider = l.provider
