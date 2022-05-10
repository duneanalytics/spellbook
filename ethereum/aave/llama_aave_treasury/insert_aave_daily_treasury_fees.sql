CREATE OR REPLACE FUNCTION aave.insert_aave_daily_treasury_fees(start_time timestamptz, end_time timestamptz) RETURNS integer
LANGUAGE plpgsql AS $function$
DECLARE r integer;
	start_time_day timestamptz := DATE_TRUNC('day',start_time);
	end_time_day timestamptz := DATE_TRUNC('day',end_time) + interval '1 day'; --since we trunc to day
BEGIN
WITH rows AS (
    INSERT INTO aave.aave_daily_treasury_fees (
	day,
	contract_address,
	borrow_fees_originated,
	repay_fees,
	liquidation_fees,
	flashloan_v1_fees,
	flashloan_v2_fees,
	swap_fees,
	lend_burn_fees,
	deployer_in,
	    version
    )
	
SELECT 
DATE_TRUNC('day',"evt_block_time") AS day,
contract_address,
SUM(CASE WHEN event = 'Borrow' THEN fee ELSE 0 END) AS borrow_fees_originated,
SUM(CASE WHEN event = 'Repay' THEN fee ELSE 0 END) AS repay_fees,
SUM(CASE WHEN event = 'Liquidation' THEN fee ELSE 0 END) AS liquidation_fees,
SUM(CASE WHEN event = 'FlashLoan' AND version = 'V1' THEN fee ELSE 0 END) AS flashloan_v1_fees,
SUM(CASE WHEN event = 'FlashLoan' AND version = 'V2' THEN fee ELSE 0 END) AS flashloan_v2_fees,
SUM(CASE WHEN event = 'Swap' THEN fee ELSE 0 END) AS swap_fees,
SUM(CASE WHEN event = 'LEND Burn' THEN fee ELSE 0 END) AS lend_burn_fees,
SUM(CASE WHEN event = 'Aave Deployer' THEN fee ELSE 0 END) AS deployer_in,
 version

FROM
(WITH tran AS (
    --v1 protocol fees on repays and flash loans (originated in borrow events)
    SELECT "evt_tx_hash","evt_block_time","_originationFee" AS fee, 'Originated' AS fee_type, "_reserve" AS contract_address, /*"_reserve" AS amount_currency, "_amount" AS amount,*/ 'Borrow' AS event, 'V1' AS version FROM aave."LendingPool_evt_Borrow"
    	WHERE evt_block_time >= start_time_day AND evt_block_time <= end_time_day
    UNION ALL
    SELECT "evt_tx_hash", "evt_block_time", NULL AS fee, NULL AS fee_type, "_reserve" AS contract_address, /*"_reserve" AS amount_currency, "_amount" AS amount,*/ 'Deposit' AS event, 'V1' AS version FROM aave."LendingPool_evt_Deposit"
    	WHERE evt_block_time >= start_time_day AND evt_block_time <= end_time_day
    UNION ALL
    --protocol fee is 30% of total fee of flash loans: https://medium.com/aave/flash-loans-one-month-in-73bde954a239
    SELECT "evt_tx_hash","evt_block_time","_protocolFee" AS fee, 'Paid' AS fee_type, "_reserve" AS contract_address, /*"_reserve" AS amount_currency, "_amount" AS amount,*/ 'FlashLoan' AS event, 'V1' AS version FROM aave."LendingPool_evt_FlashLoan"
    	WHERE evt_block_time >= start_time_day AND evt_block_time <= end_time_day
    UNION ALL
    SELECT "evt_tx_hash","evt_block_time",/*"_feeLiquidated"*/ "_liquidatedCollateralForFee" AS fee, 'Paid' AS fee_type, "_collateral" AS contract_address,  /*"_collateral" AS amount_currency, "_feeLiquidated" AS amount,*/ 'Liquidation' AS event, 'V1' AS version FROM aave."LendingPool_evt_OriginationFeeLiquidated"
    	WHERE evt_block_time >= start_time_day AND evt_block_time <= end_time_day
    UNION ALL
    SELECT "evt_tx_hash","evt_block_time",NULL AS fee, NULL AS fee_type, "_reserve" AS contract_address, /*"_reserve" AS amount_currency, "_amount" AS amount,*/ 'Withdraw' AS event, 'V1' AS version FROM aave."LendingPool_evt_RedeemUnderlying"
    	WHERE evt_block_time >= start_time_day AND evt_block_time <= end_time_day
    UNION ALL
    SELECT "evt_tx_hash","evt_block_time", "_fees" AS fee, 'Paid' AS fee_type, "_reserve" AS contract_address, /*"_reserve" AS amount_currency, "_borrowBalanceIncrease" AS amount,*/ 'Repay' AS event, 'V1' AS version FROM aave."LendingPool_evt_Repay"
    	WHERE evt_block_time >= start_time_day AND evt_block_time <= end_time_day
    UNION ALL
    SELECT "evt_tx_hash","evt_block_time",NULL AS fee, NULL AS fee_type,"_reserve" AS contract_address, /*"_reserve" AS amount_currency, "_borrowBalanceIncrease" AS amount,*/ 'Swap' AS event, 'V1' AS version FROM aave."LendingPool_evt_Swap"
    	WHERE evt_block_time >= start_time_day AND evt_block_time <= end_time_day
    --There's these weird 'OneSplit' inbound transactions.
    UNION ALL
    SELECT
    "evt_tx_hash","evt_block_time","value" AS fee, 'Paid' AS fee_type, "contract_address" AS contract_address, 'Swap' AS event, 'V1' AS version
    FROM erc20."ERC20_evt_Transfer" e
    WHERE e."from" IN ( '\x1814222fa8c8c1c1bf380e3bbfbd9de8657da476' --Uniswap
                        ,'\x7c66550c9c730b6fdd4c03bc2e73c5462c5f7acc' --Kyber
                        ,'\x65bf64ff5f51272f729bdcd7acfb00677ced86cd' --Kyber
                    )
    AND e."to" IN (SELECT address FROM llama.llama_treasury_addresses WHERE protocol = 'Aave' AND version = 'V1')
    AND e."contract_address" != '\x80fb784b7ed66730e8b1dbd9820afd29931aab03' --excluding because LEND gets burned
	AND e.evt_block_time >= start_time_day AND e.evt_block_time <= end_time_day
    
    UNION ALL
    SELECT
    "evt_tx_hash","evt_block_time","value" AS fee, 'Paid' AS fee_type, "contract_address" AS contract_address, 'LEND Burn' AS event, 'V1' AS version
    FROM erc20."ERC20_evt_Transfer" e
    WHERE e."from" IN ( '\x1814222fa8c8c1c1bf380e3bbfbd9de8657da476' --Uniswap
                        ,'\x7c66550c9c730b6fdd4c03bc2e73c5462c5f7acc' --Kyber
                        ,'\x65bf64ff5f51272f729bdcd7acfb00677ced86cd' --Kyber
                    )
    AND e."to" IN (SELECT address FROM llama.llama_treasury_addresses WHERE protocol = 'Aave' AND version = 'V1')
    AND e."contract_address" = '\x80fb784b7ed66730e8b1dbd9820afd29931aab03' --only LEND so we get the revenue that's actually burnt LEND
	AND e.evt_block_time >= start_time_day AND e.evt_block_time <= end_time_day
    
    
    --v2 flash loans. v2 doesn't take any transaction-specific fees
	UNION ALL
    --nulled the amount for flashloan because it's in a different currency. later on, we can add a column for flashloan/event currency?
    SELECT l."evt_tx_hash", l."evt_block_time", NULL::numeric AS fee, 'Paid' AS fee_type, asset AS contract_address, /*"asset" AS amount_currency, "amount" AS amount,*/ 'FlashLoan' AS event, 'V2' AS version FROM aave_v2."LendingPool_evt_FlashLoan" l
    
 	WHERE l.evt_block_time >= start_time_day AND l.evt_block_time <= end_time_day
	GROUP BY 1,2,3,4,5
    
    )
    SELECT * FROM tran
    UNION ALL --pick up 'other' unmapped deployer transactions
    SELECT
    e."evt_tx_hash",e."evt_block_time",e."value" AS fee, 'Paid' AS fee_type, e."contract_address" AS contract_address, 'Aave Deployer' AS event,
    CASE WHEN e."from" = '\x2fbb0c60a41cb7ea5323071624dcead3d213d0fa' THEN 'V2' 
        WHEN e."from" = '\x3dfd23a6c5e8bbcfc9581d2e864a68feb6a076d3' THEN 'V1'
        ELSE 'n/a' END AS version
    FROM erc20."ERC20_evt_Transfer" e
    WHERE e."from" IN ( '\x2fbb0c60a41cb7ea5323071624dcead3d213d0fa' --v2
                        ,'\x3dfd23a6c5e8bbcfc9581d2e864a68feb6a076d3' --v1
                    )
    AND e."to" IN (SELECT address FROM llama.llama_treasury_addresses WHERE protocol = 'Aave' AND version IN ('V1','V2'))
    AND e.evt_tx_hash NOT IN (SELECT evt_tx_hash FROM tran)
 	AND e.evt_block_time >= start_time_day AND e.evt_block_time <= end_time_day
 
    ) tranb
WHERE fee > 0
AND fee_type = 'Paid'

GROUP BY 
DATE_TRUNC('day',"evt_block_time"),
contract_address,
 version

    ON CONFLICT (day, contract_address, version) DO UPDATE SET
	borrow_fees_originated = EXCLUDED.borrow_fees_originated,
	repay_fees = EXCLUDED.repay_fees,
	liquidation_fees = EXCLUDED.liquidation_fees,
	flashloan_v1_fees = EXCLUDED.flashloan_v1_fees,
	flashloan_v2_fees = EXCLUDED.flashloan_v2_fees,
	swap_fees = EXCLUDED.swap_fees,
	lend_burn_fees = EXCLUDED.lend_burn_fees,
	deployer_in = EXCLUDED.deployer_in
	
    RETURNING 1
)
SELECT count(*) INTO r from rows;
RETURN r;
END
$function$;

-- Get the table started
SELECT aave.insert_aave_daily_treasury_fees(DATE_TRUNC('day','2020-01-24'::timestamptz),DATE_TRUNC('day',NOW()) )
WHERE NOT EXISTS (
    SELECT *
    FROM aave.aave_daily_treasury_fees
);

INSERT INTO cron.job (schedule, command)
VALUES ('15,45 * * * *', $$
    SELECT aave.insert_aave_daily_treasury_fees(
        (SELECT DATE_TRUNC('day',NOW()) - interval '3 days'),
        (SELECT DATE_TRUNC('day',NOW()) );
	
$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
