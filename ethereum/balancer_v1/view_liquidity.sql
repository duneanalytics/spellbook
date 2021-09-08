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
            SUM(sample_size) > 3
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
    pools AS (
        SELECT
            pool AS pools
        FROM
            balancer."BFactory_evt_LOG_NEW_POOL"
    ),
    joins AS (
        SELECT
            p.pools AS pool,
            date_trunc('day', e.evt_block_time) AS DAY,
            e.contract_address AS token,
            SUM(value) AS amount
        FROM
            erc20."ERC20_evt_Transfer" e
            INNER JOIN pools p ON e."to" = p.pools
        GROUP BY
            1,
            2,
            3
    ),
    exits AS (
        SELECT
            p.pools AS pool,
            date_trunc('day', e.evt_block_time) AS DAY,
            e.contract_address AS token,
            - SUM(value) AS amount
        FROM
            erc20."ERC20_evt_Transfer" e
            INNER JOIN pools p ON e."from" = p.pools
        GROUP BY
            1,
            2,
            3
    ),
    daily_delta_balance AS (
        SELECT
            DAY,
            pool,
            token,
            SUM(COALESCE(amount, 0)) AS amount
        FROM
            (
                SELECT
                    *
                FROM
                    joins j
                UNION
                ALL
                SELECT
                    *
                FROM
                    exits e
            ) foo
        GROUP BY
            1,
            2,
            3
    ),
    cumulative_balance AS (
        SELECT
            DAY,
            pool,
            token,
            LEAD(DAY, 1, NOW()) OVER (
                PARTITION BY token,
                pool
                ORDER BY
                    DAY
            ) AS day_of_next_change,
            SUM(amount) OVER (
                PARTITION BY pool,
                token
                ORDER BY
                    DAY ROWS BETWEEN UNBOUNDED PRECEDING
                    AND CURRENT ROW
            ) AS cumulative_amount
        FROM
            daily_delta_balance
    ),
    weekly_delta_balance_by_token AS (
        SELECT
            DAY,
            pool,
            token,
            cumulative_amount,
            (
                cumulative_amount - COALESCE(
                    LAG(cumulative_amount, 1) OVER (
                        PARTITION BY pool,
                        token
                        ORDER BY
                            DAY
                    ),
                    0
                )
            ) AS amount
        FROM
            (
                SELECT
                    DAY,
                    pool,
                    token,
                    SUM(cumulative_amount) AS cumulative_amount
                FROM
                    cumulative_balance b
                WHERE
                    extract(
                        dow
                        FROM
                            DAY
                    ) = 1
                GROUP BY
                    1,
                    2,
                    3
            ) foo
    ),
    calendar AS (
        SELECT
            generate_series(
                '2020-01-01' :: timestamp,
                CURRENT_DATE,
                '1 day' :: INTERVAL
            ) AS DAY
    ),
    cumulative_usd_balance AS (
        SELECT
            c.day,
            b.pool,
            b.token,
            cumulative_amount / 10 ^ t.decimals * COALESCE(p1.price, p2.price, 0) AS amount_usd
        FROM
            calendar c
            LEFT JOIN cumulative_balance b ON b.day <= c.day
            AND c.day < b.day_of_next_change
            LEFT JOIN erc20.tokens t ON t.contract_address = b.token
            LEFT JOIN prices p1 ON p1.day = b.day
            AND p1.token = b.token
            LEFT JOIN dex_prices p2 ON p2.day <= c.day
            AND c.day < p2.day_of_next_change
            AND p2.token = b.token
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
            INNER JOIN pools_tokens_weights w ON b.pool = w.pool_id
            AND w.normalized_weight > 0
            LEFT JOIN erc20.tokens t ON t.contract_address = w.token_address
            LEFT JOIN pool_labels p ON p.pool_id = w.pool_id
    )
    SELECT
        *
    FROM
        balancer_liquidity
);

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

COMMIT;