CREATE OR REPLACE FUNCTION aave.insert_aave_daily_treasury_events(start_time timestamptz, end_time timestamptz) RETURNS integer
LANGUAGE plpgsql AS $function$
DECLARE r integer;
	start_time_day timestamptz := DATE_TRUNC('day',start_time);
	end_time_day timestamptz := DATE_TRUNC('day',end_time) + interval '1 day'; --since we trunc to day
BEGIN
WITH rows AS (
    INSERT INTO aave.aave_daily_treasury_events (
contract_address,
version,
evt_day,
difference,
money_out_raw,
money_in_raw,
transfer_out,
transfer_in,
burn_out,
mint_in,
rewards_in,
staking_out,
staking_in,
money_in,
money_out,

borrow_fees_originated,
repay_fees,
flashloan_v1_fees,
flashloan_v2_fees,
liquidation_fees,
swap_fees,
lend_burn_fees,
deployer_in,
other_fees,

swap_out,
swap_in,
gas_out
    )
	
WITH addresses AS (
SELECT address FROM llama.llama_treasury_addresses WHERE protocol = 'Aave' AND blockchain = 'Ethereum'
)

, eth_transfers AS 
(
    SELECT '\xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee'::bytea AS contract_address, --aave's placeholder for eth
    block_time, 
    value, ABS(value) AS abs_value,
    tr_type,
    addr, g.version
    
    FROM
    (
    
    --gas only if unsuccessful
    SELECT block_time, COALESCE(-(gas_price*gas_used),0) AS value,
    'Gas Out' AS tr_type, t."from" AS addr
    FROM ethereum.transactions t
    WHERE t."from" IN (SELECT "address" FROM llama."llama_treasury_addresses"
                        WHERE "blockchain" = 'Ethereum' AND "protocol" = 'Aave')
    AND "success" = false
    AND block_time >= start_time_day AND block_time <= end_time_day
    
    UNION ALL
    
    SELECT block_time, value AS value,
    CASE WHEN l."from" IN (SELECT "address" FROM llama."llama_treasury_addresses" --different for transfers
                                WHERE "blockchain" = 'Ethereum' AND "protocol" = 'Aave')
    
        THEN 'Transfer In'
        ELSE 'Money In'
        END AS tr_type, l."to" AS addr
    FROM ethereum.traces l
    WHERE l."to" IN (SELECT address FROM addresses)
    AND (call_type NOT IN ('delegatecall', 'callcode', 'staticcall') or call_type is null)
    AND success = true
    AND "tx_success" = true
    AND block_time >= start_time_day AND block_time <= end_time_day
    
    
    UNION ALL
    
    SELECT block_time, -value AS value,
    CASE WHEN l."to" IN (SELECT "address" FROM llama."llama_treasury_addresses" --different for transfers
                                WHERE "blockchain" = 'Ethereum' AND "protocol" = 'Aave')
        THEN 'Transfer Out'
        ELSE 'Money Out'
        END AS tr_type, l."from" AS addr
    FROM ethereum.traces l
    WHERE l."from" IN (SELECT address FROM addresses)
    AND (call_type NOT IN ('delegatecall', 'callcode', 'staticcall') or call_type is null)
    AND success = true
    AND "tx_success" = true
    AND block_time >= start_time_day AND block_time <= end_time_day
    
    ) a
INNER JOIN llama.llama_treasury_addresses g
    ON a.addr = g.address
    AND g.blockchain = 'Ethereum'
    AND g."protocol" = 'Aave'
WHERE ABS(value) >0
)

SELECT b.contract_address, b.version, b.evt_day, b.difference, b.money_out_raw, b.money_in_raw,
b.transfer_out,b.transfer_in, b.burn_out, b.mint_in, b.rewards_in, b.staking_out, b.staking_in,
b.money_in - COALESCE(deployer_in,0) AS money_in --don't count deployer as rev
, b.money_out,
--money in breakout
COALESCE(borrow_fees_originated,0) AS borrow_fees_originated,
COALESCE(repay_fees,0) AS repay_fees,
COALESCE(flashloan_v1_fees,0) AS flashloan_v1_fees, --check a flashloan event vs a transaction? Maybe I'm pulling the wrong asset?
COALESCE(flashloan_v2_fees,0) AS flashloan_v2_fees,
COALESCE(liquidation_fees,0) AS liquidation_fees,
COALESCE(swap_fees,0) AS swap_fees,
COALESCE(lend_burn_fees,0) AS lend_burn_fees,
COALESCE(deployer_in,0) AS deployer_in,
COALESCE(money_in,0) -
(
COALESCE(repay_fees,0)+COALESCE(flashloan_v1_fees,0)+COALESCE(liquidation_fees,0)+
COALESCE(swap_fees,0)+COALESCE(deployer_in,0)
+mint_in+rewards_in+staking_in
) --money earned in
AS other_fees
--added later
,swap_out, swap_in, gas_out
FROM (
SELECT
COALESCE(m.address,a."contract_address") AS contract_address,  --handle for token migrations
--evt_tx_hash,--counterparty,
--ag_address,
version,
DATE_TRUNC('day',evt_block_time) AS evt_day,

SUM(value/COALESCE(m.ratio,1)) AS difference,
SUM(CASE WHEN tr_type ='Money Out' THEN value/COALESCE(m.ratio,1) ELSE 0 END) AS money_out_raw,
SUM(CASE WHEN tr_type ='Money In' THEN value/COALESCE(m.ratio,1) ELSE 0 END) AS money_in_raw,
SUM(CASE WHEN tr_type ='Transfer Out' THEN value/COALESCE(m.ratio,1) ELSE 0 END) AS transfer_out,
SUM(CASE WHEN tr_type ='Transfer In' THEN value/COALESCE(m.ratio,1) ELSE 0 END) AS transfer_in,
SUM(CASE WHEN tr_type ='Burn Out' THEN value/COALESCE(m.ratio,1) ELSE 0 END) AS burn_out,
SUM(CASE WHEN tr_type ='Mint In' THEN value/COALESCE(m.ratio,1) ELSE 0 END) AS mint_in,
SUM(CASE WHEN tr_type ='Rewards In' THEN value/COALESCE(m.ratio,1) ELSE 0 END) AS rewards_in,
SUM(CASE WHEN tr_type ='Staking Out' THEN value/COALESCE(m.ratio,1) ELSE 0 END) AS staking_out,
SUM(CASE WHEN tr_type ='Staking In' THEN value/COALESCE(m.ratio,1) ELSE 0 END) AS staking_in,
SUM(CASE WHEN tr_type ='Swap Out' THEN value/COALESCE(m.ratio,1) ELSE 0 END) AS swap_out,
SUM(CASE WHEN tr_type ='LEND In' THEN value/COALESCE(m.ratio,1) ELSE 0 END) AS swap_in,

SUM(CASE WHEN tr_type IN('Money In','Mint In') THEN value/COALESCE(m.ratio,1) ELSE 0 END) AS money_in, --protocol rev
SUM(CASE WHEN tr_type IN('Money Out','Burn Out') THEN value/COALESCE(m.ratio,1) ELSE 0 END) AS money_out, --protocol exp
SUM(CASE WHEN tr_type ='Gas Out' THEN value ELSE 0 END) AS gas_out

FROM
(
    SELECT t."contract_address",
    t."evt_block_time", --t.evt_tx_hash, t."to" AS counterparty, -- trs from treasury
    (t.value*(-1))::decimal AS value, t.value AS abs_value,
    CASE WHEN t."to" IN (SELECT address FROM addresses) THEN 'Transfer Out'
    WHEN t."evt_tx_hash" IN (SELECT "evt_tx_hash" FROM aave."stkAAVE_evt_Staked"
                                WHERE "from" IN (SELECT address FROM addresses) ) THEN 'Staking Out'
    /*WHEN t."to" IN ('\xa1116930326D21fB917d5A27F1E9943A9595fb47', --Balancer Pool
                    '\x4da27a545c0c5b758a6ba100e3a049001de870f5') --stkAAVE
                    THEN 'Staking Reward' --Staking Reward*/
    WHEN t."to" IN ( '\x1814222fa8c8c1c1bf380e3bbfbd9de8657da476' --Uniswap
                    ,'\x7c66550c9c730b6fdd4c03bc2e73c5462c5f7acc' --Kyber
                    ,'\x65bf64ff5f51272f729bdcd7acfb00677ced86cd' --Kyber
                ) THEN 'Swap Out'
    WHEN t."to" = '\x0000000000000000000000000000000000000000' THEN 'Burn Out' --these are all burning LEND tokens? But I need these to get balances to match up
    ELSE 'Money Out' END AS tr_type,
    g.address AS ag_address, g.version
    FROM erc20."ERC20_evt_Transfer" t
    INNER JOIN llama.llama_treasury_addresses g
    ON t."from" = g.address
    AND g.blockchain = 'Ethereum'
    AND g."protocol" = 'Aave'
    
    WHERE t."from" IN (SELECT address FROM addresses)
    AND t.evt_block_time >= start_time_day AND t.evt_block_time <= end_time_day
    
    UNION ALL
    
    SELECT tb."contract_address", 
    tb."evt_block_time", --tb.evt_tx_hash, tb."from" AS counterparty, -- trs to treasury
    (tb.value*(1))::decimal AS value, tb.value AS abs_value,
    CASE WHEN tb."from" IN (SELECT address FROM addresses) THEN 'Transfer In'
    WHEN tb."evt_tx_hash" IN (SELECT "evt_tx_hash" FROM aave_v2."IncentivesController_evt_RewardsClaimed"
                                WHERE "to" IN (SELECT address FROM addresses) ) THEN 'Rewards In' --claiming liquidity mining
    WHEN tb."evt_tx_hash" IN (SELECT "call_tx_hash" FROM aave."stkAAVE_call_claimRewards"
                                WHERE "to" IN (SELECT address FROM addresses) ) THEN 'Rewards In' --claiming staking rewards
    WHEN tb."evt_tx_hash" IN (SELECT "evt_tx_hash" FROM aave."stkAAVE_evt_Redeem"
                                WHERE "to" IN (SELECT address FROM addresses) ) THEN 'Staking In' --redeeming
    WHEN tb."from" = '\x0000000000000000000000000000000000000000' THEN 'Mint In' --aToken minting - we want this it's the 'share of interest'
    WHEN tb."from" IN ( '\x1814222fa8c8c1c1bf380e3bbfbd9de8657da476' --Uniswap
                    ,'\x7c66550c9c730b6fdd4c03bc2e73c5462c5f7acc' --Kyber
                    ,'\x65bf64ff5f51272f729bdcd7acfb00677ced86cd' --Kyber
                ) THEN 'LEND In'
    ELSE 'Money In' END AS tr_type,
    g.address AS ag_address, g.version
    FROM erc20."ERC20_evt_Transfer" tb
    INNER JOIN llama.llama_treasury_addresses g
    ON tb."to" = g.address
    AND g.blockchain = 'Ethereum'
    AND g."protocol" = 'Aave'
    
    WHERE tb."to" IN (SELECT address FROM addresses)
    AND tb.evt_block_time >= start_time_day AND tb.evt_block_time <= end_time_day
    
    UNION ALL
    
    SELECT * FROM eth_transfers
    
) a
LEFT JOIN llama.llama_token_migrations m
    ON a.contract_address = m.old_address

GROUP BY 1,2,3
) b
LEFT JOIN aave.aave_daily_treasury_fees at --fee generating events
ON DATE_TRUNC('day',at.day) = b.evt_day
AND at.contract_address = b.contract_address
AND LOWER(at.version) = LOWER(b.version)


    ON CONFLICT (contract_address, version, evt_day) DO UPDATE SET
    
	difference = EXCLUDED.difference,
	money_out_raw = EXCLUDED.money_out_raw,
	money_in_raw = EXCLUDED.money_in_raw,
	transfer_out = EXCLUDED.transfer_out,
	transfer_in = EXCLUDED.transfer_in,
	burn_out = EXCLUDED.burn_out,
	mint_in = EXCLUDED.mint_in,
	rewards_in = EXCLUDED.rewards_in,
	staking_out = EXCLUDED.staking_out,
	staking_in = EXCLUDED.staking_in,
	money_in = EXCLUDED.money_in,
	money_out = EXCLUDED.money_out,

	borrow_fees_originated = EXCLUDED.borrow_fees_originated,
	repay_fees = EXCLUDED.repay_fees,
	flashloan_v1_fees = EXCLUDED.flashloan_v1_fees,
	flashloan_v2_fees = EXCLUDED.flashloan_v2_fees,
	liquidation_fees = EXCLUDED.liquidation_fees,
	swap_fees = EXCLUDED.swap_fees,
	lend_burn_fees = EXCLUDED.lend_burn_fees,
	deployer_in = EXCLUDED.deployer_in,
	other_fees = EXCLUDED.other_fees,

	swap_out = EXCLUDED.swap_out,
	swap_in = EXCLUDED.swap_in,
	gas_out = EXCLUDED.gas_out
	
    RETURNING 1
)
SELECT count(*) INTO r from rows;
RETURN r;
END
$function$;

-- Get the table started --2020
SELECT aave.insert_aave_daily_treasury_events(DATE_TRUNC('day','2020-01-24'::timestamptz),DATE_TRUNC('day','2020-12-31'::timestamptz) )
WHERE NOT EXISTS (
    SELECT *
    FROM aave.aave_daily_treasury_events
	WHERE evt_day >= '2021-01-01'::timestamptz
);
--2021
SELECT aave.insert_aave_daily_treasury_events(DATE_TRUNC('day','2021-01-01'::timestamptz),DATE_TRUNC('day','2021-12-31'::timestamptz) )
WHERE NOT EXISTS (
    SELECT *
    FROM aave.aave_daily_treasury_events
	WHERE evt_day >= '2021-01-01'::timestamptz
);

SELECT aave.insert_aave_daily_treasury_events(DATE_TRUNC('day','2022-01-01'::timestamptz),DATE_TRUNC('day',NOW()::timestamptz) )
WHERE NOT EXISTS (
    SELECT *
    FROM aave.aave_daily_treasury_events
	WHERE evt_day >= '2022-01-01'::timestamptz
);

INSERT INTO cron.job (schedule, command)
VALUES ('17,47 * * * *', $$
    SELECT aave.insert_aave_daily_treasury_events(
        (SELECT DATE_TRUNC('day',NOW()) - interval '3 days'),
        (SELECT DATE_TRUNC('day',NOW()) );
	
$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
