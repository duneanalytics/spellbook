{{ config(
       schema = 'evaa_ton'
       , alias = 'withdraw'
       , materialized = 'incremental'
       , file_format = 'delta'
       , incremental_strategy = 'merge'
       , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')]
       , unique_key = ['tx_hash', 'block_date']
       , filtering_columns = ['block_date'],
    post_hook='{{ expose_spells(\'["ton"]\',
                                   "project",
                                   "evaa",
                                   \'["pshuvalov"]\') }}'
   )
 }}


-- Legacy log::withdraw_success (0x02) and V9 log::supply_withdraw_success (0x16).
-- V9 keeps withdrawal asset data in the second root reference, as V4 does.
-- https://github.com/duneanalytics/spellbook/pull/8824
-- https://github.com/evaafi/contracts/blob/d5a6bf889f8bbfa8bcc82671c17e65a3b2b360cd/contracts/core/master-withdrawal.fc#L187-L203

WITH evaa_ton_pools AS (
    {{ evaa_ton_pools() }}
),
source_data AS (
    SELECT M.block_date, M.tx_hash, M.trace_id, M.tx_now, M.tx_lt, pool_address, pool_name, tx_lt <= v4_upgrate_lt AS is_pre_v4, bitwise_right_shift(opcode, 24) AS log_type, body_boc
    FROM {{ source('ton', 'messages') }} M
    JOIN evaa_ton_pools ON M.source = pool_address
    WHERE M.direction = 'out' AND M.destination IS NULL -- ext out message
    AND M.block_date >= TIMESTAMP '2023-10-09' -- protocol launch
    {% if is_incremental() %}
        AND {{ incremental_predicate('M.block_date') }}
    {% endif %}
    AND bitwise_right_shift(opcode, 24) IN (2, 22) -- withdraw_success or supply_withdraw_success
), parse_output_prev4 as (
select CASE WHEN log_type = 2 AND is_pre_v4 THEN {{ ton_from_boc('body_boc', [
    ton_begin_parse(),
    ton_load_uint(8, 'opcode'),
    ton_load_address('owner_address'),
    ton_load_address('sender_address'),
    ton_load_uint(32, '_current_time'),
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
    ]) }} END as result, * FROM source_data WHERE log_type = 2 AND is_pre_v4
), parse_output_postv4 as (
select CASE WHEN log_type IN (2, 22) AND NOT is_pre_v4 THEN {{ ton_from_boc('body_boc', [
    ton_begin_parse(),
    ton_load_uint(8, 'opcode'),
    ton_load_address('owner_address'),
    ton_load_address('sender_address'),
    ton_load_address('recipient_address'),
    ton_load_uint(32, '_current_time'),
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
    ]) }} END as result, * FROM source_data WHERE NOT is_pre_v4
)
select block_date, tx_hash, trace_id, tx_now, tx_lt, pool_address, pool_name,
result.owner_address, result.sender_address, null AS  recipient_address, result.asset_id,
result.withdraw_amount_current, result.user_new_principal, result.new_total_supply,
result.new_total_borrow,
CAST(result.s_rate AS bigint) AS s_rate, CAST(result.b_rate AS bigint) AS b_rate -- should be less than 2^64
FROM parse_output_prev4

UNION ALL

select block_date, tx_hash, trace_id, tx_now, tx_lt, pool_address, pool_name,
result.owner_address, result.sender_address, result.recipient_address, result.asset_id,
result.withdraw_amount_current, result.user_new_principal, result.new_total_supply,
result.new_total_borrow,
CAST(result.s_rate AS bigint) AS s_rate, CAST(result.b_rate AS bigint) AS b_rate -- should be less than 2^64
FROM parse_output_postv4
WHERE log_type = 2 OR result.withdraw_amount_current > 0
