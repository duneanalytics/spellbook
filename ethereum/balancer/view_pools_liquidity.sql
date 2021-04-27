BEGIN;

DROP MATERIALIZED VIEW IF EXISTS balancer.view_pools_liquidity;

CREATE MATERIALIZED VIEW balancer.view_pools_liquidity AS (
    WITH prices AS (
        SELECT date_trunc('day', minute) AS day, contract_address AS token, AVG(price) AS price
        FROM prices.usd
        GROUP BY 1, 2
    ),
    
    dex_prices_1 AS (
        SELECT date_trunc('day', hour) AS day, 
        contract_address AS token, 
        (PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY median_price)) AS price,
        SUM(sample_size) as sample_size
        FROM dex.view_token_prices
        GROUP BY 1, 2
        HAVING sum(sample_size) > 2
    ),
    
    dex_prices AS (
        SELECT *, LEAD(day, 1, now()) OVER (PARTITION BY token ORDER BY day) AS day_of_next_change
        FROM dex_prices_1
    ),
    
    cumulative_usd_balance_by_token AS (
        SELECT b.pool, b.day, b.token, 
        cumulative_amount /10 ^ t.decimals * p1.price AS amount_usd_from_api,
        cumulative_amount /10 ^ t.decimals * p2.price AS amount_usd_from_dex
        FROM balancer.view_balances b
        LEFT JOIN erc20.tokens t ON t.contract_address = b.token
        LEFT JOIN prices p1 ON p1.day = b.day AND p1.token = b.token
        LEFT JOIN dex_prices p2 ON p2.day <= b.day AND b.day < p2.day_of_next_change AND p2.token = b.token
    ),
    
    pool_liquidity_estimates AS (
        SELECT 
            b.*, 
            b.amount_usd_from_api / w.normalized_weight AS liquidity_from_api,
            b.amount_usd_from_dex / w.normalized_weight AS liquidity_from_dex
        FROM cumulative_usd_balance_by_token b 
        INNER JOIN balancer.view_pools_tokens_weights w
        ON b.pool = w.pool_id
        AND b.token = w.token_address
        AND (b.amount_usd_from_api > 0 OR b.amount_usd_from_dex > 0)
        AND w.normalized_weight > 0
    ),
    
    estimated_pool_liquidity as (
        SELECT 
            day, 
            pool, 
            COALESCE(AVG(liquidity_from_api), AVG(liquidity_from_dex)) AS liquidity
        FROM pool_liquidity_estimates
        GROUP BY 1, 2
    )

    SELECT * FROM estimated_pool_liquidity
 
);

CREATE UNIQUE INDEX IF NOT EXISTS balancer_view_pools_liquidity_day_idx ON balancer.view_pools_liquidity (day, pool);

INSERT INTO cron.job(schedule, command)
VALUES ('*/12 * * * *', $$REFRESH MATERIALIZED VIEW CONCURRENTLY balancer.view_pools_liquidity$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
COMMIT;
