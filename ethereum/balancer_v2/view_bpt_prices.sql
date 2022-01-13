BEGIN;

DROP MATERIALIZED VIEW IF EXISTS balancer_v2.view_bpt_prices;

CREATE MATERIALIZED VIEW balancer_v2.view_bpt_prices AS (
    WITH bpt_trades AS (
        SELECT
            block_time,
            bpt_address,
            bpt_amount_raw,
            bpt_amount_raw / 10 ^ erc20a.decimals AS bpt_amount,
            token_amount_raw,
            token_amount_raw / 10 ^ erc20b.decimals AS token_amount,
            p.price * token_amount_raw / 10 ^ erc20b.decimals AS usd_amount
        FROM (
            SELECT
                t.evt_block_time AS block_time,
                CASE 
                    WHEN t."tokenIn" = SUBSTRING(t."poolId" FOR 20) THEN t."tokenIn"
                    ELSE t."tokenOut"
                END AS bpt_address,
                CASE 
                    WHEN t."tokenIn" = SUBSTRING(t."poolId" FOR 20) THEN t."amountIn"
                    ELSE t."amountOut"
                END AS bpt_amount_raw,
                CASE 
                    WHEN t."tokenIn" = SUBSTRING(t."poolId" FOR 20) THEN t."tokenOut"
                    ELSE t."tokenIn"
                END AS token_address,
                CASE 
                    WHEN t."tokenIn" = SUBSTRING(t."poolId" FOR 20) THEN t."amountOut"
                    ELSE t."amountIn"
                END AS token_amount_raw
            FROM balancer_v2."Vault_evt_Swap" t
            WHERE t."tokenIn" = SUBSTRING(t."poolId" FOR 20)
            OR t."tokenOut" = SUBSTRING(t."poolId" FOR 20)
        ) dexs
        JOIN erc20.tokens erc20a ON erc20a.contract_address = dexs.bpt_address
        JOIN erc20.tokens erc20b ON erc20b.contract_address = dexs.token_address
        LEFT JOIN prices.usd p ON p.minute = date_trunc('minute', dexs.block_time)
        AND p.contract_address = dexs.token_address
    ),

    bpt_estimated_prices AS (
        SELECT
            block_time,
            bpt_address,
            usd_amount / bpt_amount AS price
        FROM
            bpt_trades
    )

    SELECT
        date_trunc('hour', block_time) as hour,
        bpt_address AS contract_address,
        (PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price)) AS median_price
    FROM bpt_estimated_prices
    GROUP BY 1, 2
);

CREATE UNIQUE INDEX IF NOT EXISTS dex_token_prices_unique ON balancer_v2.view_bpt_prices (hour, contract_address);

INSERT INTO cron.job(schedule, command)
VALUES ('* 1 * * *', $$REFRESH MATERIALIZED VIEW CONCURRENTLY balancer_v2.view_bpt_prices$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
COMMIT;