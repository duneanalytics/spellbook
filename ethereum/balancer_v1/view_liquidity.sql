BEGIN;

CREATE SCHEMA IF NOT EXISTS balancer_v1;

DROP MATERIALIZED VIEW IF EXISTS balancer_v1.view_liquidity;

CREATE MATERIALIZED VIEW balancer_v1.view_liquidity AS (
    WITH pool_labels AS (
        SELECT
            address AS pool_id,
            name AS pool_symbol
        FROM
            (
                SELECT
                    address,
                    name,
                    ROW_NUMBER() OVER (
                        PARTITION BY address
                        ORDER BY
                            MAX(updated_at) DESC
                    ) AS num
                FROM
                    labels.labels
                WHERE
                    "type" IN ('balancer_pool')
                GROUP BY
                    1,
                    2
            ) l
        WHERE
            num = 1
    ),
    prices AS (
        SELECT
            date_trunc('day', MINUTE) AS DAY,
            contract_address AS token,
            AVG(price) AS price
        FROM
            prices.usd
        GROUP BY
            1,
            2
    ),
    dex_prices_1 AS (
        SELECT
            date_trunc('day', HOUR) AS DAY,
            contract_address AS token,
            (
                PERCENTILE_DISC(0.5) WITHIN GROUP (
                    ORDER BY
                        median_price
                )
            ) AS price,
            SUM(sample_size) AS sample_size
        FROM
            dex.view_token_prices
        GROUP BY
            1,
            2
        HAVING
            SUM(sample_size) > 5
            AND AVG(median_price) < 1e8
    ),
    dex_prices AS (
        SELECT
            *,
            LEAD(DAY, 1, NOW()) OVER (
                PARTITION BY token
                ORDER BY
                    DAY
            ) AS day_of_next_change
        FROM
            dex_prices_1
    ),
    cumulative_usd_balance AS (
        SELECT
            b.day,
            b.pool,
            b.token,
            cumulative_amount / 10 ^ t.decimals * COALESCE(p1.price, p2.price, 0) AS amount_usd
        FROM
            balancer.view_balances b
            LEFT JOIN erc20.tokens t ON t.contract_address = b.token
            LEFT JOIN prices p1 ON p1.day = b.day
            AND p1.token = b.token
            LEFT JOIN dex_prices p2 ON p2.day <= b.day
            AND b.day < p2.day_of_next_change
            AND p2.token = b.token
            WHERE pool != '\xBA12222222228d8Ba445958a75a0704d566BF2C8'
    ),
    pools_tokens_weights AS (
        SELECT
            *
        FROM
            balancer."view_pools_tokens_weights"
    ),
    pool_liquidity_estimates AS (
        SELECT
            b.day,
            b.pool,
            SUM(b.amount_usd) / SUM(w.normalized_weight) AS liquidity
        FROM
            cumulative_usd_balance b
            INNER JOIN pools_tokens_weights w ON b.pool = w.pool_id
            AND b.token = w.token_address
            AND b.amount_usd > 0
            AND w.normalized_weight > 0
        GROUP BY
            1,
            2
    ),
    balancer_liquidity AS (
        SELECT
            DAY,
            w.pool_id,
            pool_symbol,
            token_address,
            symbol AS token_symbol,
            liquidity * normalized_weight AS usd_amount
        FROM
            pool_liquidity_estimates b
            LEFT JOIN pools_tokens_weights w ON b.pool = w.pool_id
            AND w.normalized_weight > 0
            LEFT JOIN erc20.tokens t ON t.contract_address = w.token_address
            LEFT JOIN pool_labels p ON p.pool_id = w.pool_id
    )
    SELECT
        *
    FROM
        balancer_liquidity
);

COMMIT;

CREATE UNIQUE INDEX IF NOT EXISTS balancer_view_liquidity_idx ON balancer_v1.view_liquidity (DAY, pool_id, token_address);

CREATE INDEX IF NOT EXISTS balancer_view_liquidity_day_idx ON balancer_v1.view_liquidity USING BRIN (DAY);

CREATE INDEX IF NOT EXISTS balancer_view_liquidity_pool_idx ON balancer_v1.view_liquidity (pool_id);

CREATE INDEX IF NOT EXISTS balancer_view_liquidity_token_idx ON balancer_v1.view_liquidity (token_address);

INSERT INTO
    cron.job(schedule, command)
VALUES
    (
        '*/12 * * * *',
        $$REFRESH MATERIALIZED VIEW CONCURRENTLY balancer_v1.view_liquidity$$
    ) ON CONFLICT (command) DO
UPDATE
SET
    schedule = EXCLUDED.schedule;