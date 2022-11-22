CREATE TABLE IF NOT EXISTS aave.deposit (   
    version text,
    transaction_type text,
    symbol text,
    token bytea,
    contract_address bytea,
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
      contract_address,
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
      erc20.contract_address,
      depositor,
      withdrawn_to,
      liquidator,
      amount / (10^erc20.decimals) AS amount,
      (amount/(10^p.decimals)) * price AS usd_amount,
      evt_tx_hash,
      evt_index,
      evt_block_time,
      evt_block_number
    
FROM (

SELECT 
    '1' AS version,
    'deposit' AS transaction_type,
    CASE
        WHEN _reserve = '\xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE' THEN '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' 
        ELSE _reserve
    END AS token,
    "_user" AS depositor, 
    NULL::bytea AS withdrawn_to,
    NULL::bytea AS liquidator,
    _amount AS amount,
    evt_tx_hash,
    evt_index,
    evt_block_time,
    evt_block_number
FROM aave."LendingPool_evt_Deposit" 
WHERE evt_block_time >= start_ts
AND evt_block_time < end_ts
AND evt_block_number >= start_block
AND evt_block_number < end_block
UNION ALL 
-- all withdrawals
SELECT 
    '1' AS version,
    'withdraw' AS transaction_type,
    CASE
        WHEN _reserve = '\xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE' THEN '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' 
        ELSE _reserve
    END AS token,
    "_user" AS depositor,
    "_user" AS withdrawn_to,
    NULL::bytea AS liquidator,
    - _amount AS amount,
    evt_tx_hash,
    evt_index,
    evt_block_time,
    evt_block_number
FROM aave."LendingPool_evt_RedeemUnderlying"
WHERE evt_block_time >= start_ts
AND evt_block_time < end_ts
AND evt_block_number >= start_block
AND evt_block_number < end_block
UNION ALL 
-- liquidation
SELECT 
    '1' AS version,
    'deposit_liquidation' AS transaction_type,
    CASE
        WHEN "_collateral" = '\xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE' THEN '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' 
        ELSE "_collateral"
    END AS token,
    "_user" AS depositor,
    _liquidator AS withdrawn_to,
    _liquidator AS liquidator,
    - "_liquidatedCollateralAmount" AS amount,
    evt_tx_hash,
    evt_index,
    evt_block_time,
    evt_block_number
FROM aave."LendingPool_evt_LiquidationCall"
WHERE evt_block_time >= start_ts
AND evt_block_time < end_ts
AND evt_block_number >= start_block
AND evt_block_number < end_block
UNION ALL
SELECT 
    '2' AS version,
    'deposit' AS transaction_type,
    reserve AS token,
    "user" AS depositor, 
    NULL::bytea as withdrawn_to,
    NULL::bytea AS liquidator,
    amount, 
    evt_tx_hash,
    evt_index,
    evt_block_time,
    evt_block_number
FROM aave_v2."LendingPool_evt_Deposit" 
WHERE evt_block_time >= start_ts
AND evt_block_time < end_ts
AND evt_block_number >= start_block
AND evt_block_number < end_block
UNION ALL 
-- all withdrawals
SELECT 
    '2' AS version,
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
FROM aave_v2."LendingPool_evt_Withdraw"
WHERE evt_block_time >= start_ts
AND evt_block_time < end_ts
AND evt_block_number >= start_block
AND evt_block_number < end_block
UNION ALL 
-- liquidation
SELECT 
    '2' AS version,
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
FROM aave_v2."LendingPool_evt_LiquidationCall"
WHERE evt_block_time >= start_ts
AND evt_block_time < end_ts
AND evt_block_number >= start_block
AND evt_block_number < end_block
) deposit
LEFT JOIN erc20."tokens" erc20
    ON deposit.token = erc20.contract_address
LEFT JOIN prices.usd p 
    ON p.minute = date_trunc('minute', deposit.evt_block_time) 
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
        (SELECT MAX(number) FROM ethereum.blocks WHERE time < (SELECT MAX(evt_block_time) - interval '1 days' FROM aave.deposit)),
        (SELECT MAX(number) FROM ethereum.blocks where time < now() - interval '20 minutes'));
$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
