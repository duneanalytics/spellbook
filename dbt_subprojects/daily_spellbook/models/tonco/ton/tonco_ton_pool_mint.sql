{{ config(
       schema = 'tonco_ton'
       , alias = 'pool_mint'
       , materialized = 'incremental'
       , file_format = 'delta'
       , incremental_strategy = 'merge'
       , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')]
       , unique_key = ['tx_hash', 'block_date']
       , post_hook='{{ expose_spells(\'["ton"]\',
                                   "project",
                                   "tonco",
                                   \'["markfromton"]\') }}'
   )
 }}

-- based on reference docs: https://docs.tonco.io/technical-reference/contracts/pool

WITH 
pools as (
    SELECT DISTINCT pool as address
    FROM 
        {{ source('ton', 'dex_pools') }}
    WHERE 
        project = 'tonco'
),
parsed_boc AS (
    SELECT 
        M.block_date, 
        M.tx_hash, 
        M.trace_id, 
        M.tx_now, 
        M.tx_lt, 
        M.destination as pool_address, 
        body_boc
    FROM 
        {{ source('ton', 'messages') }} M
    WHERE 
        M.block_date >= TIMESTAMP '2024-11-01'
        {% if is_incremental() %}
            AND {{ incremental_predicate('M.block_date') }}
        {% endif %}
        AND M.direction = 'out'
        AND M.destination in (select address from pools)
        AND M.opcode = -2123354376 -- 0x81702ef8
), 
parse_output AS (
    -- -2123354376: 0x81702ef8, POOLV3_MINT
    SELECT 
        {{ ton_from_boc('body_boc', [
            ton_begin_parse(),
            ton_load_uint(32, 'op'),
            ton_load_uint(64, 'query_id'),
            ton_load_coins('amount0Funded'),
            ton_load_coins('amount1Funded'),
            ton_load_address('recipient'),
            ton_load_uint(128, 'liquidity'),
            ton_load_int(24, 'tickLower'),
            ton_load_int(24, 'tickUpper')
        ]) }} as result,
        * FROM parsed_boc
)
SELECT 
    block_date, 
    tx_hash, 
    trace_id, 
    tx_now, 
    tx_lt,  
    pool_address,
    result.op,
    result.query_id,
    result.amount0Funded,
    result.amount1Funded,
    result.recipient,
    result.liquidity,
    result.tickLower,
    result.tickUpper
FROM parse_output