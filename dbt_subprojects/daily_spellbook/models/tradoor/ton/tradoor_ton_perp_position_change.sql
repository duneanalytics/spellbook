{{ config(
       schema = 'tradoor_ton'
       , alias = 'perp_position_change'
       , materialized = 'incremental'
       , file_format = 'delta'
       , incremental_strategy = 'merge'
       , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')]
       , unique_key = ['tx_hash', 'block_date']
       , post_hook='{{ expose_spells(\'["ton"]\',
                                   "project",
                                   "tradoor",
                                   \'["pshuvalov"]\') }}'
   )
 }}


-- based on this implementation https://github.com/ton-studio/ton-etl/blob/e733691ebe61c24444acb622e5ad5b2fa317b352/parser/parsers/message/tradoor_trades.py

WITH tradoor_pools AS (
    SELECT pool_address, pool_name FROM (VALUES 
    (upper('0:ff1338c9f6ed1fa4c264a19052bff64d10c8ad028628f52b2e0f4b357a12227e'), 'USDT-v2'),
    (upper('0:0d36ba31cc15d776dd529b990872735972b0c4ceec77741f9ed3344e48e19084'), 'TON-v2'),
    (upper('0:1b31de77fbf382b1023ad114d383e191506366d6e14af8c6264699081d3a2309'), 'USDT-v3')
    ) AS T(pool_address, pool_name)
),
parsed_boc AS (
    SELECT M.block_date, M.tx_hash, M.trace_id, M.tx_now, M.tx_lt, pool_address, pool_name, M.source as amm, body_boc
    FROM {{ source('ton', 'messages') }} M
    JOIN tradoor_pools ON M.source = pool_address
    WHERE M.block_date > TIMESTAMP '2024-09-01'
    {% if is_incremental() %}
        AND {{ incremental_predicate('M.block_date') }}
    {% endif %}
    AND (opcode = 1197042366 or opcode = 592660044) -- position_increased or position_decreased
    AND M.direction = 'out'
    AND M.destination is null -- ext-out
), parse_output as (
select {{ ton_from_boc('body_boc', [
    ton_begin_parse(),
    ton_load_uint(32, 'opcode'),
    ton_load_uint(64, 'trx_id'),
    ton_load_uint(64, 'order_id'),
    ton_load_uint(8, 'op_type'),
    ton_load_uint(64, 'position_id'),
    ton_load_address('trader_addr'),
    ton_load_uint(16, 'token_id'),
    ton_load_uint(1, 'is_long'),
    ton_load_uint(128, 'margin_delta'),
    ton_load_coins('margin_after'),
    ton_load_uint(128, 'size_delta'),
    ton_load_coins('size_after'),
    ton_load_ref(),
    ton_begin_parse(),
    ton_load_uint(128, 'trade_price'),
    ton_load_uint(128, 'entry_price')
    ]) }} as result, * from parsed_boc
)
select block_date, tx_hash, trace_id, tx_now, tx_lt,
pool_address, pool_name, case when result.opcode = 1197042366 then 1 else 0 end as is_increased, result.trx_id, result.order_id, result.op_type,
result.position_id, result.trader_addr, result.token_id, result.is_long, result.margin_delta,
result.margin_after, result.size_delta, result.size_after, result.trade_price, result.entry_price
from parse_output