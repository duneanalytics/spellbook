CREATE OR REPLACE FUNCTION dex.insert_weth_balances(date_to_add timestamptz) RETURNS integer
LANGUAGE plpgsql AS $function$
DECLARE r integer;
BEGIN
-- The cron job has to run after completion of `dex.insert_weth_balance_changes`
-- Get the token balance for `date_to_add` by accumulating previous balance changes until end of that day
WITH balances AS (
    SELECT
        pool_address,
        token_address,
        SUM(change_amount_raw) AS token_amount_raw
    FROM dex.daily_balance_changes
    WHERE day < (SELECT date_to_add + interval '1 day')
    GROUP BY 1, 2
),
rows AS (
    INSERT INTO dex.liquidity (
        day,
        token_symbol,
        token_amount,
        pool_name,
        project,
        version,
        category,
        token_amount_raw,
        token_usd_amount,
        token_address,
        pool_address,
        token_index,
        token_pool_percentage
    )
    SELECT
        date_to_add AS day,
        dexs.token_symbol,
        dexs.token_amount_raw / 10 ^ 18 AS token_amount,
        info.pool_name,
        info.project,
        info.version,
        info.category,
        dexs.token_amount_raw,
        dexs.token_amount_raw / (10 ^ 18) * p.price AS usd_amount,
        dexs.token_address,
        dexs.pool_address,
        dexs.token_index,
        dexs.token_pool_percentage
    FROM (
        SELECT
            'WETH' AS token_symbol,
            token_amount_raw,
            token_address,
            pool_address,
            NULL AS token_index
            NULL AS token_pool_percentage
        FROM balances
    ) dexs
    -- Choose mid-day price as an approximation of token price for that day to calculate `usd_amount`
    LEFT JOIN prices.usd p on p.contract_address = dexs.token_address and p.minute = (SELECT date_to_add + interval '12 hours')
    LEFT JOIN dex.view_lp_pools_info info ON dexs.pool_address = info.pool_address

    ON CONFLICT DO NOTHING
    RETURNING 1
)
SELECT count(*) INTO r from rows;
RETURN r;
END
$function$;


-- Daily cron job updates entries for previous day
INSERT INTO cron.job (schedule, command)
VALUES ('43 4 * * *', $$
    SELECT dex.insert_weth_balances(
        (SELECT date_trunc('day', now()) - interval '1 day'));
$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
