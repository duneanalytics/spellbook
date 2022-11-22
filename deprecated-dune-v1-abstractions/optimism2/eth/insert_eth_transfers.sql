CREATE OR REPLACE FUNCTION eth.insert_eth_transfers(start_block_time timestamptz, end_block_time timestamptz=now()) RETURNS integer
LANGUAGE plpgsql AS $function$
DECLARE r integer;
BEGIN
WITH rows AS (
    INSERT INTO eth.eth_transfers (
       "from",
        "to",
	    contract_address,
        value,
        value_decimal,
        tx_hash,
        trace_address,
        tx_block_time,
        tx_block_number,
	    tx_method_id
    )
    
    SELECT 
    r."from",
    r."to",
	'\xDeadDeAddeAddEAddeadDEaDDEAdDeaDDeAD0000'::bytea AS	contract_address, --Using the ETH deposit placeholder address to match with prices tables
    r.value,
    r.value/1e18 AS value_decimal,
    r."tx_hash",
    r."trace_address",
    r."block_time" AS tx_block_time,
    r."block_number" AS tx_block_number,
    substring(t.data from 1 for 4) AS tx_method_id
    FROM optimism."traces" r
	INNER JOIN optimism.transactions t
            ON t.hash = r.tx_hash
    WHERE (r.call_type NOT IN ('delegatecall', 'callcode', 'staticcall') or r.call_type is null)
    AND r."tx_success" = true
    AND r.success = true
    AND r.value > 0    
    AND r.block_time >= start_block_time
    AND r.block_time < end_block_time	
	AND t.block_time >= start_block_time
    AND t.block_time < end_block_time
	
	UNION ALL --ETH Transfers from deposits and withdrawals are ERC20 transfers of the 'deadeadead' ETH token. These do not appear in traces.
	
	SELECT 
    r."from",
    r."to",
	'\xDeadDeAddeAddEAddeadDEaDDEAdDeaDDeAD0000'::bytea AS	contract_address, --Using the ETH deposit placeholder address to match with prices tables
    r.value,
    r.value/1e18 AS value_decimal,
    r."evt_tx_hash" AS tx_hash,
    array[r."evt_index"::int] AS "trace_address",
    r."evt_block_time" AS tx_block_time,
    r."evt_block_number" AS tx_block_number,
    substring(t.data from 1 for 4) AS tx_method_id
    FROM erc20."ERC20_evt_Transfer" r
	INNER JOIN optimism.transactions t
            ON t.hash = r.evt_tx_hash

    WHERE contract_address = '\xDeadDeAddeAddEAddeadDEaDDEAdDeaDDeAD0000'
    AND t.success = true
    AND r.value > 0    
    AND r.evt_block_time >= start_block_time
    AND r.evt_block_time < end_block_time	
	AND t.block_time >= start_block_time
    AND t.block_time < end_block_time

    ON CONFLICT (tx_hash, trace_address)
    DO UPDATE SET
	value = EXCLUDED.value,
	value_decimal = EXCLUDED.value_decimal

    RETURNING 1
)
SELECT count(*) INTO r from rows;
RETURN r;
END
$function$;

-- -- fill post-regenesis 11-11
-- SELECT eth.insert_eth_transfers(
--     '2021-11-11'::timestamptz,
--     '2022-01-01'::timestamptz
-- )
-- WHERE NOT EXISTS (
--     SELECT *
--     FROM eth.eth_transfers
--     WHERE tx_block_time >= '2021-11-11'::timestamptz
--     AND tx_block_time < '2022-01-01'::timestamptz
-- )
-- ;

-- --fill 2022
-- SELECT eth.insert_eth_transfers(
--     '2022-01-01'::timestamptz,
--     '2022-05-01'::timestamptz
-- )
-- WHERE NOT EXISTS (
--     SELECT *
--     FROM eth.eth_transfers
--     WHERE tx_block_time >= '2022-01-01'::timestamptz
--     AND tx_block_time < '2022-02-01'::timestamptz
-- )
-- ;

-- INSERT INTO cron.job (schedule, command)
-- VALUES ('* * * * *', $$
--     SELECT eth.insert_eth_transfers(
--         (SELECT max(tx_block_time) FROM eth.eth_transfers WHERE tx_block_time > NOW() - interval '1 month'),
--         (SELECT now())
--         );
-- $$)
-- ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
