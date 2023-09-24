WITH 

get_calls_balances as ( 
    SELECT 
        account as wallet_address,
        CAST(output_0 as double) as amount_raw,
        call_block_time as block_time,
        date_trunc('hour', call_block_time) as block_hour,
        call_block_number as block_number,
        call_trace_address as trace_address,
        call_tx_hash as tx_hash,
        ROW_NUMBER() OVER (PARTITION BY account, date_trunc('hour', call_block_time) ORDER BY call_block_number DESC, call_trace_address DESC NULLS LAST) as row_number
    FROM 
    {{ source('circle_polygon', 'UChildAdministrableERC20_call_balanceOf') }}
    WHERE call_success
), 

balances_calls as (
        SELECT 
            * 
        FROM
        get_calls_balances
        WHERE row_number = 1 
), 

get_spell_balances as (
    SELECT 
        bh.block_hour,  
        bh.amount_raw as amount_raw_spell, 
        bc.amount_raw as amount_raw_calls,
        bh.wallet_address,
        bc.block_time,
        bc.tx_hash
    FROM 
    {{ ref('balances_polygon_erc20_hour') }} bh 
    INNER JOIN 
    balances_calls bc 
        ON bh.wallet_address = bc.wallet_address
        AND bh.block_hour = bc.block_hour
    WHERE bh.token_address = 0x2791bca1f2de4661ed88a30c99a7a9449aa84174
),

hours_addresses as (
    SELECT 
        agg.block_hour, 
        agg.wallet_address, 
        gs.tx_hash 
    FROM 
    get_spell_balances gs 
    INNER JOIN 
    {{ ref('transfers_polygon_erc20_agg_hour') }}  agg 
        ON gs.wallet_address = agg.wallet_address
        AND gs.block_hour = agg.block_hour 
    WHERE agg.token_address = 0x2791bca1f2de4661ed88a30c99a7a9449aa84174
),

filter_txns_after_calls as (
    SELECT
        * 
    FROM
    get_spell_balances
    WHERE tx_hash NOT IN (SELECT tx_hash FROM hours_addresses)
),

test as (
    SELECT 
        *, 
        CASE WHEN amount_raw_spell = amount_raw_calls THEN true ELSE false END as value_test 
    FROM    
    filter_txns_after_calls
)

SELECT 
    * 
FROM 
test
WHERE value_test = true