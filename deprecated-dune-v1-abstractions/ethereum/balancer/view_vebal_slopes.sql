BEGIN;
DROP VIEW IF EXISTS balancer.view_vebal_slopes;

CREATE VIEW balancer.view_vebal_slopes AS

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
            evt_block_number AS block_number,
            evt_block_time AS block_time,
            SUM(value/1e18) AS delta_bpt
        FROM balancer."veBAL_evt_Deposit"
        GROUP BY 1, 2, 3
    ),
    
    withdrawals AS (
        SELECT 
            provider,
            evt_block_number AS block_number,
            evt_block_time AS block_time,
            -SUM(value/1e18) AS delta_bpt
        FROM balancer."veBAL_evt_Withdraw"
        GROUP BY 1, 2, 3
    ),
    
    bpt_locked_balance AS (
        SELECT block_number, block_time, provider, SUM(delta_bpt) AS bpt_balance
        FROM (
            SELECT * FROM deposits
            UNION ALL
            SELECT * FROM withdrawals
        ) union_all
        GROUP BY 1, 2, 3
    ),
    
    cumulative_balances AS (
        SELECT
            block_number,
            block_time,
            provider,
            SUM(bpt_balance) OVER (
                PARTITION BY provider
                ORDER BY block_number
                ROWS BETWEEN
                UNBOUNDED PRECEDING AND
                CURRENT ROW
            ) AS bpt_balance
        FROM bpt_locked_balance b
    )

SELECT
    block_number,
    block_time,
    FLOOR(EXTRACT(EPOCH FROM block_time)) AS block_timestamp,
    b.provider,
    bpt_balance,
    unlocked_at,
    bpt_balance / (365*86400) AS slope,
    (unlocked_at - FLOOR(EXTRACT(EPOCH FROM block_time))) * bpt_balance / (365*86400) AS bias
FROM cumulative_balances b
LEFT JOIN locks_info l
ON l.provider = b.provider
AND l.updated_at = (
    SELECT MAX(updated_at)
    FROM locks_info
    WHERE updated_at <= (FLOOR(EXTRACT(EPOCH FROM block_time)))
    AND provider = b.provider
);
COMMIT;