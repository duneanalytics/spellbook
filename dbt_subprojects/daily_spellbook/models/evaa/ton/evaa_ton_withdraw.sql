{{ config(
       schema = 'evaa_ton'
       , alias = 'withdraw'
       , materialized = 'incremental'
       , file_format = 'delta'
       , incremental_strategy = 'merge'
       , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')]
       , unique_key = ['tx_hash']
       , post_hook='{{ expose_spells(\'["ton"]\',
                                   "project",
                                   "evaa",
                                   \'["pshuvalov"]\') }}'
   )
 }}


-- log::withdraw_success is sent by the router and contains all the data regarding the withdrawal
-- https://github.com/evaafi/contracts/blob/d5a6bf889f8bbfa8bcc82671c17e65a3b2b360cd/contracts/core/master-withdrawal.fc#L187-L203

WITH evaa_ton_pools AS (
    {{ evaa_ton_pools() }}
),
parsed_boc AS (
    SELECT M.block_date, M.tx_hash, M.trace_id, M.tx_now, M.tx_lt, pool_address, body_boc
    FROM {{ source('ton', 'messages') }} M
    JOIN evaa_ton_pools ON M.source = pool_address
    WHERE M.direction = 'out' AND M.destination IS NULL -- ext out message
    -- AND T.block_date >= TIMESTAMP '2023-10-09' -- protocol launch
    AND M.block_date = TIMESTAMP '2025-03-11' -- protocol launch
    {% if is_incremental() %}
        AND {{ incremental_predicate('M.block_date') }}
    {% endif %}
    AND bitwise_right_shift(opcode, 24) = 2 -- log::withdraw_success, 0x2
), parse_output as (
select {{ ton_from_boc('body_boc', [
    ton_begin_parse(),
    ton_load_uint(8, 'opcode'),
    ton_load_address('owner_address'),
    ton_load_address('sender_address'),
    ton_load_address('recipient_address'),
    ton_load_uint(32, 'current_time'),
    ton_skip_refs(1),
    ton_load_ref(),
    ton_begin_parse(),
    ton_load_uint(256, 'asset_id'),
    ton_load_uint(64, 'withdraw_amount_current'),
    ton_load_int(64, 'user_new_principal'),
    ton_load_int(64, 'new_total_supply'),
    ton_load_int(64, 'new_total_borrow'),
    ton_load_uint(64, 's_rate'),
    ton_load_uint(64, 'b_rate')
    ]) }} as result, * from parsed_boc
)
select CAST(result['s_rate'] AS bigint) as s_rate from parse_output