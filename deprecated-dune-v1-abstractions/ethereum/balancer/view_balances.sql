BEGIN;

DROP MATERIALIZED VIEW IF EXISTS balancer.view_balances;

CREATE MATERIALIZED VIEW balancer.view_balances AS (
    WITH pools AS (
        SELECT pool as pools
        FROM balancer."BFactory_evt_LOG_NEW_POOL"
    ),
    
    joins AS (
        SELECT p.pools as pool, date_trunc('day', e.evt_block_time) AS day, e.contract_address AS token, SUM(value) AS amount
        FROM erc20."ERC20_evt_Transfer" e
        INNER JOIN pools p ON e."to" = p.pools
        GROUP BY 1, 2, 3

        UNION ALL

        SELECT e."to" as pool, date_trunc('day', e.evt_block_time) AS day, e.contract_address AS token, SUM(value) AS amount
        FROM erc20."ERC20_evt_Transfer" e
        WHERE e."to" = '\xBA12222222228d8Ba445958a75a0704d566BF2C8'
        GROUP BY 1, 2, 3

    ),

    exits AS (
        SELECT p.pools as pool, date_trunc('day', e.evt_block_time) AS day, e.contract_address AS token, -SUM(value) AS amount
        FROM erc20."ERC20_evt_Transfer" e
        INNER JOIN pools p ON e."from" = p.pools   
        GROUP BY 1, 2, 3

        UNION ALL

        SELECT e."from" as pool, date_trunc('day', e.evt_block_time) AS day, e.contract_address AS token, -SUM(value) AS amount
        FROM erc20."ERC20_evt_Transfer" e
        WHERE e."from" = '\xBA12222222228d8Ba445958a75a0704d566BF2C8'
        GROUP BY 1, 2, 3
    ),
    
    daily_delta_balance_by_token AS (
        SELECT pool, day, token, SUM(COALESCE(amount, 0)) AS amount FROM 
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
            LEAD(day, 1, now()) OVER (PARTITION BY token, pool ORDER BY day) AS day_of_next_change,
            SUM(amount) OVER (PARTITION BY pool, token ORDER BY day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_amount
        FROM daily_delta_balance_by_token
    ),
    
    calendar AS (
        SELECT generate_series('2020-01-01'::timestamp, CURRENT_DATE, '1 day'::interval) AS day
    ),
    
    running_cumulative_balance_by_token AS (
        SELECT c.day, pool, token, cumulative_amount 
        FROM calendar c
        LEFT JOIN cumulative_balance_by_token b ON b.day <= c.day AND c.day < b.day_of_next_change
    )
    
    SELECT * FROM running_cumulative_balance_by_token
    
);

CREATE UNIQUE INDEX IF NOT EXISTS balancer_view_balances_day_idx ON balancer.view_balances (day, token, pool);

INSERT INTO cron.job(schedule, command)
VALUES ('*/12 * * * *', $$REFRESH MATERIALIZED VIEW CONCURRENTLY balancer.view_balances$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
COMMIT;
