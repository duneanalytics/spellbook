CREATE TABLE IF NOT EXISTS aave.interest (   
    version text,
    day timestamptz,
    symbol text,
    token bytea,
    deposit_apy numeric,
    stable_borrow_apy numeric,
    variable_borrow_apy numeric,
    daily_deposit_apr numeric,
    daily_stable_borrow_apr numeric,
    daily_variable_borrow_apr numeric,
    PRIMARY KEY (token, day)
    
);

CREATE OR REPLACE FUNCTION aave.insert_interest(start_ts timestamptz, end_ts timestamptz=now(), start_block numeric=0, end_block numeric=9e18) RETURNS integer
LANGUAGE plpgsql AS $function$
DECLARE r integer;
BEGIN
WITH rows AS (
    INSERT INTO aave.interest (
    version,
    day,
    symbol,
    token,
    deposit_apy,
    stable_borrow_apy,
    variable_borrow_apy,
    daily_deposit_apr,
    daily_stable_borrow_apr,
    daily_variable_borrow_apr
    )
    ((SELECT
    '3' AS version,
    day,
    symbol,
    reserve AS token,
    deposit_apy,
    stable_borrow_apy,
    variable_borrow_apy,
    deposit_apr AS daily_deposit_apr,
    stable_borrow_apr AS daily_stable_borrow_apr,
    variable_borrow_apr AS daily_variable_borrow_apr
FROM (
SELECT 
    gs.day,
    reserve,
    deposit_apy,
    stable_borrow_apy,
    variable_borrow_apy,
    deposit_apr,
    stable_borrow_apr,
    variable_borrow_apr
FROM (
SELECT
    day,
    lead(day, 1, DATE_TRUNC('day',now() + '1 day'::interval) ) OVER (PARTITION BY "reserve"
                            ORDER BY day asc) AS next_day,
    reserve,
    deposit_apy,
    stable_borrow_apy,
    variable_borrow_apy,
    ((1+deposit_apy)^(1.0/365.0)-1) AS deposit_apr, 
    ((1+stable_borrow_apy)^(1.0/365.0)-1) AS stable_borrow_apr,
    ((1+variable_borrow_apy)^(1.0/365.0)-1) AS variable_borrow_apr
FROM (
SELECT 
    date_trunc('day', evt_block_time) AS day,
    reserve,
    AVG("liquidityRate" / 1e27) AS deposit_apy,
    AVG("stableBorrowRate" / 1e27) AS stable_borrow_apy,
    AVG("variableBorrowRate" /1e27) AS variable_borrow_apy
FROM aave_v3."Pool_evt_ReserveDataUpdated"
WHERE evt_block_time >= start_ts
AND evt_block_time < end_ts
AND evt_block_number >= start_block
AND evt_block_number < end_block 
GROUP BY 1, 2
) o
) day
INNER JOIN 
(SELECT generate_series('2020-01-01', NOW(), '1 day') AS day) gs -- to select gap days
ON day.day <= gs.day
AND gs.day < day.next_day
) i
LEFT JOIN (
    SELECT 
        contract_address, 
        tokens.decimals,
        CASE 
		WHEN (symbol = 'ETH'::text) THEN 'WETH'::text ELSE symbol
        END AS "symbol"
    FROM erc20.tokens
) erc20tokens
ON i.reserve = erc20tokens.contract_address

    
    ))
    ON CONFLICT DO NOTHING
    RETURNING 1
)
SELECT count(*) INTO r from rows;
RETURN r;
END
$function$;

SELECT aave.insert_interest(DATE_TRUNC('day','2019-01-24'::timestamptz),DATE_TRUNC('day',NOW()) )
WHERE NOT EXISTS (
    SELECT *
    FROM aave.interest
);

INSERT INTO cron.job (schedule, command)
VALUES ('14,44 * * * *', $$
    SELECT aave.insert_interest(
        (SELECT DATE_TRUNC('day',NOW()) - interval '3 days'),
        (SELECT DATE_TRUNC('day',NOW()) );
	
$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
