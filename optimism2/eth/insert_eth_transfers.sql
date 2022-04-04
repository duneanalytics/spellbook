CREATE OR REPLACE FUNCTION dune_user_generated.eth_insert_eth_transfers(start_block_time timestamptz, end_block_time timestamptz=now()) RETURNS integer
LANGUAGE plpgsql AS $function$
DECLARE r integer;
BEGIN
WITH rows AS (
    INSERT INTO dune_user_generated.eth_eth_transfers (
       "from",
        "to",
        raw_value,
        value,
        value_decimal,
        tx_hash,
        tx_index,
        tx_block_time,
        tx_block_number
    )
    
    SELECT 
    "from",
    "to",
    value,
    value/1e18 AS value_decimal,
    "tx_hash",
    "tx_index",
    "block_time" AS tx_block_time,
    "block_number" AS tx_block_number,
    substring(input from 1 for 4) AS tx_method_id
        FROM optimism."traces"
        WHERE (call_type NOT IN ('delegatecall', 'callcode', 'staticcall') or call_type is null)
        AND "tx_success" = true
        AND success = true
        AND value > 0
        
        AND block_time >= start_block_time
        AND block_time < end_block_time

	
    -- update if we have new info on prices or the erc20
    ON CONFLICT (trace_tx_hash, trace_index)
    DO NOTHING

    RETURNING 1
)
SELECT count(*) INTO r from rows;
RETURN r;
END
$function$;
