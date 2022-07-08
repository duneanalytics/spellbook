BEGIN;

CREATE SCHEMA IF NOT EXISTS balancer_v2;

DROP MATERIALIZED VIEW IF EXISTS balancer_v2.view_liquidity;

CREATE MATERIALIZED VIEW balancer_v2.view_liquidity AS (
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
                    "type" IN ('balancer_v2_pool')
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
            sum(sample_size) > 3
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
    swaps_changes AS (
        SELECT
            DAY,
            pool_id,
            token,
            SUM(COALESCE(delta, 0)) AS delta
        FROM
            (
                SELECT
                    date_trunc('day', evt_block_time) AS DAY,
                    "poolId" AS pool_id,
                    "tokenIn" AS token,
                    "amountIn" AS delta
                FROM
                    balancer_v2."Vault_evt_Swap"
                UNION
                ALL
                SELECT
                    date_trunc('day', evt_block_time) AS DAY,
                    "poolId" AS pool_id,
                    "tokenOut" AS token,
                    - "amountOut" AS delta
                FROM
                    balancer_v2."Vault_evt_Swap"
            ) swaps
        GROUP BY
            1,
            2,
            3
    ),
    balances_changes AS (
        SELECT
            date_trunc('day', evt_block_time) AS DAY,
            "poolId" AS pool_id,
            UNNEST(tokens) AS token,
            UNNEST(deltas) - UNNEST("protocolFeeAmounts") AS delta
        FROM
            balancer_v2."Vault_evt_PoolBalanceChanged"
    ),
    managed_changes AS (
        SELECT
            date_trunc('day', evt_block_time) AS DAY,
            "poolId" AS pool_id,
            token,
            "cashDelta" + "managedDelta" AS delta
        FROM
            balancer_v2."Vault_evt_PoolBalanceManaged"
    ),
    daily_delta_balance AS (
        SELECT
            DAY,
            pool_id,
            token,
            SUM(COALESCE(amount, 0)) AS amount
        FROM
            (
                SELECT
                    DAY,
                    pool_id,
                    token,
                    SUM(COALESCE(delta, 0)) AS amount
                FROM
                    balances_changes
                GROUP BY
                    1,
                    2,
                    3
                UNION
                ALL
                SELECT
                    DAY,
                    pool_id,
                    token,
                    delta AS amount
                FROM
                    swaps_changes
                UNION
                ALL
                SELECT
                    DAY,
                    pool_id,
                    token,
                    delta AS amount
                FROM
                    managed_changes
            ) balance
        GROUP BY
            1,
            2,
            3
    ),
    cumulative_balance AS (
        SELECT
            DAY,
            pool_id,
            token,
            LEAD(DAY, 1, NOW()) OVER (
                PARTITION BY token,
                pool_id
                ORDER BY
                    DAY
            ) AS day_of_next_change,
            SUM(amount) OVER (
                PARTITION BY pool_id,
                token
                ORDER BY
                    DAY ROWS BETWEEN UNBOUNDED PRECEDING
                    AND CURRENT ROW
            ) AS cumulative_amount
        FROM
            daily_delta_balance
    ),
    calendar AS (
        SELECT
            generate_series(
                '2021-04-21' :: timestamp,
                CURRENT_DATE,
                '1 day' :: INTERVAL
            ) AS DAY
    ),
    cumulative_usd_balance AS (
        SELECT
            c.day,
            b.pool_id,
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
        WHERE b.token != SUBSTRING(b.pool_id FOR 20)
    ),
    pools_tokens_weights AS (
        SELECT
            c."poolId" AS pool_id,
            unnest(cc.tokens) AS token_address,
            unnest(cc.weights) / 1e18 AS normalized_weight
        FROM
            balancer_v2."Vault_evt_PoolRegistered" c
            INNER JOIN balancer_v2."WeightedPoolFactory_call_create" cc ON c.evt_tx_hash = cc.call_tx_hash
        UNION
        ALL
        SELECT
            c."poolId" AS pool_id,
            unnest(cc.tokens) AS token_address,
            unnest(cc.weights) / 1e18 AS normalized_weight
        FROM
            balancer_v2."Vault_evt_PoolRegistered" c
            INNER JOIN balancer_v2."WeightedPool2TokensFactory_call_create" cc ON c.evt_tx_hash = cc.call_tx_hash
    ),
    pool_liquidity_estimates AS (
        SELECT
            b.day,
            b.pool_id,
            SUM(b.amount_usd) / COALESCE(SUM(w.normalized_weight), 1) AS liquidity
        FROM
            cumulative_usd_balance b
            LEFT JOIN pools_tokens_weights w ON b.pool_id = w.pool_id
            AND b.token = w.token_address
        GROUP BY
            1,
            2
    ),
    balancer_liquidity AS (
        SELECT
            b.day,
            b.pool_id,
            pool_symbol,
            token AS token_address,
            symbol AS token_symbol,
            coalesce(amount_usd, liquidity * normalized_weight) AS usd_amount
        FROM
            pool_liquidity_estimates b
            LEFT JOIN cumulative_usd_balance c ON c.day = b.day
            AND c.pool_id = b.pool_id
            LEFT JOIN pools_tokens_weights w ON b.pool_id = w.pool_id
            AND w.token_address = c.token
            LEFT JOIN erc20.tokens t ON t.contract_address = c.token
            LEFT JOIN pool_labels p ON p.pool_id = SUBSTRING(b.pool_id :: text, 0, 43) :: bytea
    )
    SELECT
        *
    FROM
        balancer_liquidity
);

CREATE UNIQUE INDEX IF NOT EXISTS balancer_v2_view_liquidity_idx ON balancer_v2.view_liquidity (DAY, pool_id, token_address);

CREATE INDEX IF NOT EXISTS balancer_v2_view_liquidity_day_idx ON balancer_v2.view_liquidity USING BRIN (DAY);

CREATE INDEX IF NOT EXISTS balancer_v2_view_liquidity_pool_idx ON balancer_v2.view_liquidity (pool_id);

CREATE INDEX IF NOT EXISTS balancer_v2_view_liquidity_token_idx ON balancer_v2.view_liquidity (token_address);

INSERT INTO
    cron.job (schedule, command)
VALUES
    (
        '*/12 * * * *',
        $$REFRESH MATERIALIZED VIEW CONCURRENTLY balancer_v2.view_liquidity$$
    ) ON CONFLICT (command) DO
UPDATE
SET
    schedule = EXCLUDED.schedule;

COMMIT;