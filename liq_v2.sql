WITH prices AS (
        SELECT
            date_trunc('day', minute) AS day,
            contract_address AS token,
            AVG(price) AS price
        FROM prices.usd
        WHERE blockchain = 'ethereum'
        AND minute >= DATE_TRUNC('day', NOW() - interval '1 day')
        AND contract_address IN ('0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2', '0xba100000625a3754423978a60c9317c58a424e3d')
        GROUP BY 1, 2
    ),

    dex_prices_1 AS (
        SELECT
            date_trunc('day', hour) AS day,
            contract_address AS token,
            percentile(median_price, 0.5) AS price,
            SUM(sample_size) AS sample_size
        FROM
            dex.prices
        WHERE blockchain = 'ethereum'
        AND day >= DATE_TRUNC('day', NOW() - interval '1 day')
        AND contract_address IN ('0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2', '0xba100000625a3754423978a60c9317c58a424e3d')
        GROUP BY 1, 2
        HAVING SUM(sample_size) > 5
        AND AVG(median_price) < 1e8
    ),

    dex_prices AS (
        SELECT
            *,
            LEAD(day, 1, NOW()) OVER (
                PARTITION BY token
                ORDER BY
                    day
            ) AS day_of_next_change
        FROM
            dex_prices_1
    ),

    cumulative_balance AS (
        SELECT
            day,
            pool,
            token,
            cumulative_amount
        FROM balancer_ethereum.balances b
        WHERE pool = '0x59a19d8c652fa0284f44113d0ff9aba70bd46fb4'
        AND b.day >= DATE_TRUNC('day', NOW() - interval '1 day')
    )

   -- cumulative_usd_balance AS (
        SELECT
            b.day,
            b.pool,
            b.token,
            cumulative_amount / POWER(10, t.decimals) * COALESCE(p1.price, p2.price, 0) AS amount_usd
        FROM cumulative_balance b
        LEFT JOIN tokens.erc20 t ON t.contract_address = b.token
        LEFT JOIN prices p1 ON p1.day = b.day
        AND p1.token = b.token
        LEFT JOIN dex_prices p2 ON p2.day <= b.day
        AND b.day < p2.day_of_next_change
        AND p2.token = b.token