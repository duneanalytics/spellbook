DROP TABLE balancer.view_liquidity;

CREATE TABLE balancer.view_liquidity (
    block_time timestamptz NOT NULL,
    pool_address bytea NOT NULL,
    token_address bytea,
    token_symbol text,
    token_weight numeric,
    delta_token_amount numeric,
    delta_token_amount_raw numeric,
    delta_usd_amount numeric,
    token_amount numeric,
    token_amount_raw numeric,
    usd_amount numeric,
    version text
);

CREATE UNIQUE INDEX IF NOT EXISTS view_liquidity_unique_idx ON balancer.view_liquidity (block_time, pool_address, token_address);
CREATE INDEX IF NOT EXISTS view_liquidity_block_time_idx ON balancer.view_liquidity USING BRIN (block_time);
CREATE INDEX IF NOT EXISTS view_liquidity_pool_address_idx ON balancer.view_liquidity (pool_address);
CREATE INDEX IF NOT EXISTS view_liquidity_token_address_idx ON balancer.view_liquidity (token_address);

CREATE OR REPLACE FUNCTION balancer.insert_balancer_liquidity(start_ts timestamptz, end_ts timestamptz=now()) RETURNS integer
LANGUAGE plpgsql AS $function$
DECLARE r integer;
BEGIN
WITH rows AS (
    INSERT INTO balancer.view_liquidity (
      block_time,
      pool_address,
      token_address,
      token_symbol,
      token_weight,
      delta_token_amount,
      delta_token_amount_raw,
      delta_usd_amount,
      token_amount,
      token_amount_raw,
      usd_amount,
      version
    )

    WITH prices AS (
        SELECT
            date_trunc('day', minute) AS day,
            contract_address AS token,
            AVG(price) AS price
        FROM prices.usd
        WHERE minute >= date_trunc('day', start_ts)
        AND minute < end_ts
        GROUP BY 1, 2
    ),

    dex_prices_1 AS (
        SELECT
            date_trunc('day', hour) AS day,
            contract_address AS token,
            SUM(sample_size) as sample_size,
            (PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY median_price)) AS price
        FROM dex.view_token_prices
        WHERE hour >= date_trunc('day', start_ts)
        AND hour < end_ts
        GROUP BY 1, 2
        HAVING sum(sample_size) > 2
    ),

    dex_prices AS (
        SELECT
            *,
            LEAD(day, 1, now()) OVER (PARTITION BY token ORDER BY day) AS day_of_next_change
        FROM dex_prices_1
    ),

    pools_v1 AS (
        SELECT pool
        FROM balancer."BFactory_evt_LOG_NEW_POOL"
    ),

    events_v1 AS (
        SELECT
            date_trunc('day', e.evt_block_time) AS day,
            p.pool,
            e.contract_address AS token,
            SUM(value) AS amount
        FROM erc20."ERC20_evt_Transfer" e
        INNER JOIN pools_v1 p ON e."to" = p.pool
        AND e.evt_block_time >= date_trunc('day', start_ts)
        AND e.evt_block_time < end_ts
        GROUP BY 1, 2, 3

        UNION ALL

        SELECT
            date_trunc('day', e.evt_block_time) AS day,
            p.pool,
            e.contract_address AS token,
            -SUM(value) AS amount
        FROM erc20."ERC20_evt_Transfer" e
        INNER JOIN pools_v1 p ON e."from" = p.pool
        AND e.evt_block_time >= date_trunc('day', start_ts)
        AND e.evt_block_time < end_ts
        GROUP BY 1, 2, 3
    ),

    events_v2 AS (
        SELECT
            date_trunc('day', evt_block_time) AS day,
            "poolId" AS pool,
            "tokenIn" AS token,
            "amountIn" AS delta
        FROM balancer_v2."Vault_evt_Swap"
        WHERE evt_block_time >= date_trunc('day', start_ts)
        AND evt_block_time < end_ts

        UNION ALL

        SELECT
            date_trunc('day', evt_block_time) AS day,
            "poolId" AS pool,
            "tokenOut" AS token,
            -"amountOut" AS delta
        FROM balancer_v2."Vault_evt_Swap"
        WHERE evt_block_time >= date_trunc('day', start_ts)
        AND evt_block_time < end_ts

        UNION ALL

        SELECT
            date_trunc('day', evt_block_time) AS day,
            NULL::bytea AS pool,
            token,
            SUM(COALESCE(delta, 0)) AS delta
        FROM balancer_v2."Vault_evt_InternalBalanceChanged"
        WHERE evt_block_time >= date_trunc('day', start_ts)
        AND evt_block_time < end_ts
        GROUP BY 1, 2, 3

        UNION ALL

        SELECT
            date_trunc('day', evt_block_time) AS day,
            "poolId" AS pool,
            UNNEST(tokens) AS token,
            UNNEST(deltas) AS delta
        FROM balancer_v2."Vault_evt_PoolBalanceChanged"
        WHERE evt_block_time >= date_trunc('day', start_ts)
        AND evt_block_time < end_ts

        UNION ALL

        SELECT
            date_trunc('day', evt_block_time) AS day,
            "poolId" AS pool,
            token,
            "cashDelta" + "managedDelta" AS delta
        FROM balancer_v2."Vault_evt_PoolBalanceManaged"
        WHERE evt_block_time >= date_trunc('day', start_ts)
        AND evt_block_time < end_ts

    ),

    daily_delta_balance AS (
        SELECT day, version, pool, token, SUM(COALESCE(amount, 0)) AS delta_amount
        FROM (
            SELECT '1' AS version, * FROM events_v1
            UNION ALL
            SELECT '2' AS version, * FROM events_v2
        ) balances
        GROUP BY 1, 2, 3, 4
    ),

    last_balance AS (
        SELECT token_amount_raw AS last_amount, pool_address, token_address
        FROM balancer.view_liquidity
        WHERE block_time =
        (   SELECT MAX(block_time)
            FROM balancer.view_liquidity
            WHERE date_trunc('day', block_time) = date_trunc('day', start_ts - interval '1 day')
        )
    ),

    daily_balance AS (
        SELECT
            b.day,
            b.version,
            b.pool,
            b.token,
            b.delta_amount,
            LEAD(b.day, 1, now()) OVER (PARTITION BY b.token, b.pool ORDER BY b.day) AS day_of_next_change,
            COALESCE(l.last_amount, 0) + SUM(b.delta_amount) OVER (PARTITION BY b.pool, b.token ORDER BY b.day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_amount
        FROM daily_delta_balance b
        LEFT JOIN last_balance l
        ON b.pool = l.pool_address
        AND b.token = l.token_address
    ),

    calendar AS (
        SELECT generate_series(date_trunc('day', start_ts), end_ts - interval '1 day', '1 day'::interval) AS day
    ),

    running_daily_balance AS (
        SELECT
            c.day,
            b.version,
            b.pool,
            b.token,
            b.delta_amount,
            b.cumulative_amount
        FROM calendar c
        LEFT JOIN daily_balance b
        ON b.day <= c.day
        AND c.day < b.day_of_next_change
    ),

    daily_usd_balance AS (
        SELECT
            b.day AS block_time,
            b.version,
            b.pool AS pool_address,
            b.token AS token_address,
            t.symbol AS token_symbol,
            delta_amount AS delta_token_amount_raw,
            cumulative_amount AS token_amount_raw,
            delta_amount / 10 ^ t.decimals AS delta_token_amount,
            cumulative_amount / 10 ^ t.decimals AS token_amount,
            delta_amount / 10 ^ t.decimals * COALESCE(p1.price, p2.price) AS delta_usd_amount,
            cumulative_amount / 10 ^ t.decimals * COALESCE(p1.price, p2.price) AS usd_amount
        FROM running_daily_balance b
        LEFT JOIN erc20.tokens t ON t.contract_address = b.token
        LEFT JOIN prices p1 ON p1.day = b.day AND p1.token = b.token
        LEFT JOIN dex_prices p2 ON p2.day <= b.day AND b.day < p2.day_of_next_change AND p2.token = b.token
    ),

    pools_balances AS (
        SELECT
            b.*,
            w1.normalized_weight AS token_weight
        FROM daily_usd_balance b
        INNER JOIN balancer.view_pools_tokens_weights w1
        ON b.pool_address = w1.pool_id
        AND b.token_address = w1.token_address
        AND b.usd_amount > 0
        AND w1.normalized_weight > 0
    )

    SELECT
        block_time,
        pool_address,
        token_address,
        token_symbol,
        token_weight,
        delta_token_amount,
        delta_token_amount_raw,
        delta_usd_amount,
        token_amount,
        token_amount_raw,
        usd_amount,
        version
    FROM pools_balances
    ON CONFLICT DO NOTHING
    RETURNING 1
)

SELECT count(*) INTO r from rows;
RETURN r;
END
$function$;

-- fill 2020
SELECT balancer.insert_balancer_liquidity(
    '2020-01-01',
    '2021-01-01'
)
WHERE NOT EXISTS (
    SELECT *
    FROM balancer.view_liquidity
    WHERE block_time > '2020-01-01'
    AND block_time <= '2021-01-01'
);

-- fill 2021
SELECT balancer.insert_balancer_liquidity(
    '2021-01-01',
    now()
)
WHERE NOT EXISTS (
    SELECT *
    FROM balancer.view_liquidity
    WHERE block_time > '2021-01-01'
    AND block_time <= now()
);

INSERT INTO cron.job (schedule, command)
VALUES ('*/10 * * * *', $$
    SELECT balancer.insert_balancer_liquidity(
        (SELECT max(block_time) - interval '1 days' FROM balancer.view_liquidity),
        (SELECT now()));
$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;