CREATE TABLE IF NOT EXISTS aave.deposit (   
    version text,
    transaction_type text,
    symbol text,
    token bytea,
    depositor bytea,
    withdrawn_to bytea,
    liquidator bytea,
    amount numeric,
    usd_amount numeric,
    evt_tx_hash bytea,
    evt_index integer,
    evt_block_time timestamptz,
    evt_block_number numeric,
    PRIMARY KEY (evt_tx_hash, evt_index)
);

CREATE OR REPLACE FUNCTION aave.insert_deposit(start_ts timestamptz, end_ts timestamptz=now(), start_block numeric=0, end_block numeric=9e18) RETURNS integer
LANGUAGE plpgsql AS $function$
DECLARE r integer;
BEGIN
WITH rows AS (
    INSERT INTO aave.deposit (
      version,
      transaction_type,
      symbol,
      token,
      depositor,
      withdrawn_to,
      liquidator,
      amount,
      usd_amount,
      evt_tx_hash,
      evt_index,
      evt_block_time,
      evt_block_number
      )
    ((SELECT  
      
      version,
      transaction_type,
      erc20.symbol,
      deposit.token,
      depositor,
      withdrawn_to,
      liquidator,
      amount / (10^erc20.decimals) AS amount,
      (amount/(10^p.decimals)) * median_price AS usd_amount,
      evt_tx_hash,
      evt_index,
      evt_block_time,
      evt_block_number
    
FROM (

SELECT 
    '3' AS version,
    'deposit' AS transaction_type,
    reserve AS token,
    "user" AS depositor, 
    NULL::bytea AS withdrawn_to,
    NULL::bytea AS liquidator,
    amount, 
    evt_tx_hash,
    evt_index,
    evt_block_time,
    evt_block_number
FROM aave_v3."Pool_evt_Supply" 
WHERE evt_block_time >= start_ts
AND evt_block_time < end_ts
AND evt_block_number >= start_block
AND evt_block_number < end_block 
UNION ALL 
-- all withdrawals
SELECT 
    '3' AS version,
    'withdraw' AS transaction_type,
    reserve AS token,
    "user" AS depositor,
    "to" AS withdrawn_to,
    NULL::bytea AS liquidator,
    - amount AS amount,
    evt_tx_hash,
    evt_index,
    evt_block_time,
    evt_block_number
FROM aave_v3."Pool_evt_Withdraw"
WHERE evt_block_time >= start_ts
AND evt_block_time < end_ts
AND evt_block_number >= start_block
AND evt_block_number < end_block 
UNION ALL 
-- liquidation
SELECT 
    '3' AS version,
    'deposit_liquidation' AS transaction_type,
    "collateralAsset" AS token,
    "user" AS depositor,
    liquidator AS withdrawn_to,
    liquidator AS liquidator,
    - "liquidatedCollateralAmount" AS amount,
    evt_tx_hash,
    evt_index,
    evt_block_time,
    evt_block_number
FROM aave_v3."Pool_evt_LiquidationCall"
WHERE evt_block_time >= start_ts
AND evt_block_time < end_ts
AND evt_block_number >= start_block
AND evt_block_number < end_block 
) deposit
LEFT JOIN erc20."tokens" erc20
    ON deposit.token = erc20.contract_address
LEFT JOIN prices."approx_prices_from_dex_data" p
    ON p.hour = date_trunc('hour', deposit.evt_block_time) 
    AND p.contract_address = deposit.token
))
    ON CONFLICT DO NOTHING
    RETURNING 1
)
SELECT count(*) INTO r from rows;
RETURN r;
END
$function$;

SELECT aave.insert_deposit(DATE_TRUNC('day','2019-01-24'::timestamptz),DATE_TRUNC('day',NOW()) )
WHERE NOT EXISTS (
    SELECT *
    FROM aave.deposit
);

INSERT INTO cron.job (schedule, command)
VALUES ('*/20 * * * *', $$
    SELECT aave.insert_deposit(
        (SELECT MAX(evt_block_time) - interval '1 days' FROM aave.deposit),
        (SELECT now() - interval '20 minutes'),
        (SELECT MAX(number) FROM optimism.blocks WHERE time < (SELECT MAX(evt_block_time) - interval '1 days' FROM aave.deposit)),
        (SELECT MAX(number) FROM optimism.blocks where time < now() - interval '20 minutes'));
$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
