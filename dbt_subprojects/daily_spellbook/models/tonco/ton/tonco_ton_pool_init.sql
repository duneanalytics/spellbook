{{ config(
       schema = 'tonco_ton'
       , alias = 'pool_init'
       , materialized = 'incremental'
       , file_format = 'delta'
       , incremental_strategy = 'merge'
       , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')]
       , unique_key = ['tx_hash', 'block_date']
       , post_hook='{{ expose_spells(\'["ton"]\',
                                   "project",
                                   "tonco",
                                   \'["markysha"]\') }}'
   )
 }}

-- based on reference docs: https://docs.tonco.io/technical-reference/contracts/pool

WITH 
pools as (
    SELECT DISTINCT pool as address
    FROM 
        {{ source('ton', 'dex_pools') }}
    WHERE 
        project = 'tonco' -- like this https://tonviewer.com/EQD25vStEwc-h1QT1qlsYPQwqU5IiOhox5II0C_xsDNpMVo7
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
        M.block_date >= TIMESTAMP '2024-11-01' -- Assuming same protocol launch date
        {% if is_incremental() %}
            AND {{ incremental_predicate('M.block_date') }}
        {% endif %}
        AND M.direction = 'out'
        AND M.destination in (select address from pools)
        AND M.opcode = 1142700525 -- 0x441c39ed
), 
parse_output AS (
    -- 1142700525: 0x441c39ed, POOLV3_INIT
    SELECT 
        {{ ton_from_boc('body_boc', [
            ton_begin_parse(),
            ton_load_uint(32, 'op'),
            ton_load_uint(64, 'query_id'),
            ton_load_uint(1, 'from_admin'),
            ton_load_uint(1, 'has_admin'),
            ton_load_address('admin_addr'),
            ton_load_uint(1, 'has_controller'),
            ton_load_address('controller_addr'),
            ton_load_uint(1, 'set_spacing'),
            ton_load_int(24, 'tick_spacing'),
            ton_load_uint(1, 'set_price'),
            ton_load_uint(160, 'initial_priceX96'),
            ton_load_uint(1, 'set_active'),
            ton_load_uint(1, 'pool_active'),
            ton_load_uint(16, 'protocol_fee'),
            ton_load_uint(16, 'lp_fee_base'),
            ton_load_uint(16, 'lp_fee_current'),
            ton_skip_refs(2),
            ton_load_maybe_ref(),
            ton_begin_parse(),
            ton_load_address('jetton0_minter'),
            ton_load_address('jetton1_minter')
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
    result.from_admin,
    result.has_admin,
    result.admin_addr,
    result.has_controller,
    result.controller_addr,
    result.set_spacing,
    result.tick_spacing,
    result.set_price,
    result.initial_priceX96,
    result.set_active,
    result.pool_active,
    result.protocol_fee,
    result.lp_fee_base,
    result.lp_fee_current,
    result.jetton0_minter,
    result.jetton1_minter
FROM parse_output