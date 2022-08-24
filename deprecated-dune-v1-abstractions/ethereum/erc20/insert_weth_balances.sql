CREATE OR REPLACE FUNCTION erc20.insert_weth_balances(time_to_add timestamptz) RETURNS integer
LANGUAGE plpgsql AS $function$
DECLARE r integer;
BEGIN
-- The cron job has to run after completion of `erc20.insert_weth_balance_changes`
-- Get the token balance for `time_to_add` by accumulating previous balance changes
WITH balances AS (
    SELECT
        wallet_address,
        token_address,
        SUM(amount_raw) AS amount_raw
    FROM erc20.weth_hourly_balance_changes d1
    -- Sum over all previous balance changes to get current balance
    WHERE hour <= time_to_add
        -- Only add an entry when there was a balance change for `wallet_address`
        -- and `token_address` during the hourly interval stored as `time_to_add`
        AND EXISTS (SELECT * FROM erc20.weth_hourly_balance_changes d2 
                    WHERE d2.hour = time_to_add
                        AND d1.wallet_address = d2.wallet_address
                        AND d1.token_address = d2.token_address)
    GROUP BY 1, 2
),
rows AS (
    INSERT INTO erc20.token_balances (
        "timestamp",
        wallet_address,
        token_address,
        token_symbol,
        amount_raw,
        amount
    )
    SELECT
        time_to_add AS "timestamp",
        b.wallet_address,
        b.token_address,
        b.token_symbol,
        b.amount_raw,
        b.amount_raw / 10 ^ 18 AS amount
    FROM (
        SELECT
            wallet_address,
            token_address,
            'WETH' AS token_symbol,
            amount_raw
        FROM balances
    ) b

    ON CONFLICT ON CONSTRAINT token_balances_pkey DO UPDATE SET amount_raw = EXCLUDED.amount_raw, amount = EXCLUDED.amount
    RETURNING 1
)
SELECT count(*) INTO r from rows;
RETURN r;
END
$function$;


-- Daily cron job enters entries for previous hour
INSERT INTO cron.job (schedule, command)
VALUES ('7 * * * *', $$
    SELECT erc20.insert_weth_balances(
        (SELECT date_trunc('hour', now()) - interval '2 hours'));
$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;


-- Daily cron job `upserts` entries for 24h ago
INSERT INTO cron.job (schedule, command)
VALUES ('17 * * * *', $$
    SELECT erc20.insert_weth_balances(
        (SELECT date_trunc('hour', now()) - interval '24 hours'));
$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
