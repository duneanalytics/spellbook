{{ config(
       schema = 'evaa_ton'
       , alias = 'withdraw'
       , materialized = 'incremental'
       , file_format = 'delta'
       , incremental_strategy = 'merge'
       , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')]
       , unique_key = ['tx_hash', 'block_date']
       , post_hook='{{ expose_spells(\'["ton"]\',
                                   "project",
                                   "evaa",
                                   \'["pshuvalov"]\') }}'
   )
 }}


-- log::withdraw_success is sent by the router and contains all the data regarding the withdrawal
-- https://github.com/evaafi/contracts/blob/d5a6bf889f8bbfa8bcc82671c17e65a3b2b360cd/contracts/core/master-withdrawal.fc#L187-L203
-- op::withdraw is deprecated in the EVAA V9, just uses supply/withdraw instead of it.
-- https://github.com/evaafi/contracts/blob/d9138cb24f03b53522774351aceb38c51a047eee/contracts/core/master-supply-withdrawal.fc#L718-L745

WITH evaa_ton_pools AS (
    {{ evaa_ton_pools() }}
),
source_data AS (
    SELECT M.block_date, M.tx_hash, M.trace_id, M.tx_now, M.tx_lt, pool_address, pool_name, tx_lt <= v4_upgrate_lt AS is_pre_v4, body_boc,
           bitwise_right_shift(opcode, 24) AS log_type
    FROM {{ source('ton', 'messages') }} M
    JOIN evaa_ton_pools ON M.source = pool_address
    WHERE M.direction = 'out' AND M.destination IS NULL -- ext out message
    AND M.block_date >= TIMESTAMP '2023-10-09' -- protocol launch
    {% if is_incremental() %}
        AND {{ incremental_predicate('M.block_date') }}
    {% endif %}
    AND (bitwise_right_shift(opcode, 24) = 2 OR bitwise_right_shift(opcode, 24) = 22) -- log::withdraw_success (0x2) or log::supply_withdraw_success (0x16)
), parse_output_prev4 as (
select {{ ton_from_boc('body_boc', [
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
    ]) }} as result, * FROM source_data WHERE is_pre_v4 AND log_type = 2
), parse_output_postv4 as (
select {{ ton_from_boc('body_boc', [
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
    ]) }} as result, * FROM source_data WHERE NOT is_pre_v4 AND log_type = 2
), parse_output_supply_withdraw as (
select {{ ton_from_boc('body_boc', [
    ton_begin_parse(),
    ton_load_uint(8, 'opcode'),
    ton_load_address('owner_address'),
    ton_load_address('sender_address'),
    ton_load_address('recipient_address'),
    ton_load_uint(32, '_current_time'),
    ton_load_int(16, 'subaccount_id'),
    ton_load_ref(),
    ton_begin_parse(),
    ton_load_uint(256, 'supply_asset_id'),
    ton_load_uint(64, 'supply_amount'),
    ton_load_int(64, 'supply_new_principal'),
    ton_load_int(64, 'new_total_supply_in'),
    ton_load_int(64, 'new_total_borrow_in'),
    ton_load_uint(64, 's_rate_in'),
    ton_load_uint(64, 'b_rate_in'),
    ton_end_parse(),
    ton_load_ref(),
    ton_begin_parse(),
    ton_load_uint(256, 'withdraw_asset_id'),
    ton_load_uint(64, 'withdraw_amount'),
    ton_load_int(64, 'withdraw_new_principal'),
    ton_load_int(64, 'new_total_supply_out'),
    ton_load_int(64, 'new_total_borrow_out'),
    ton_load_uint(64, 's_rate_out'),
    ton_load_uint(64, 'b_rate_out')
    ]) }} as result, * FROM source_data WHERE log_type = 22
), supply_from_supply_withdraw as (
    -- Extract supply records from supply_withdraw transactions when supply_amount > 0
    SELECT block_date, tx_hash, trace_id, tx_now, tx_lt, pool_address, pool_name,
           result.owner_address, result.sender_address, result.recipient_address, 
           result.supply_asset_id as asset_id,
           result.supply_amount as amount_supplied, 
           result.supply_new_principal as user_new_principal, 
           result.new_total_supply_in as new_total_supply,
           result.new_total_borrow_in as new_total_borrow,
           CAST(result.s_rate_in AS bigint) AS s_rate, 
           CAST(result.b_rate_in AS bigint) AS b_rate,
           'supply_withdraw' as transaction_type
    FROM parse_output_supply_withdraw
    WHERE result.supply_amount > 0
), withdraw_from_supply_withdraw as (
    -- Extract withdraw records from supply_withdraw transactions
    SELECT block_date, tx_hash, trace_id, tx_now, tx_lt, pool_address, pool_name,
           result.owner_address, result.sender_address, result.recipient_address, 
           CAST(result.subaccount_id AS bigint) AS subaccount_id,
           result.withdraw_asset_id as asset_id,
           result.withdraw_amount as withdraw_amount_current, 
           result.withdraw_new_principal as user_new_principal, 
           result.new_total_supply_out as new_total_supply,
           result.new_total_borrow_out as new_total_borrow,
           CAST(result.s_rate_out AS bigint) AS s_rate, 
           CAST(result.b_rate_out AS bigint) AS b_rate,
           'supply_withdraw' as transaction_type
    FROM parse_output_supply_withdraw
)
-- Withdraw records from regular withdraw transactions
select block_date, tx_hash, trace_id, tx_now, tx_lt, pool_address, pool_name,
result.owner_address, result.sender_address, null AS  recipient_address, CAST(NULL AS int) AS subaccount_id, result.asset_id,
result.withdraw_amount_current, result.user_new_principal, result.new_total_supply,
result.new_total_borrow,
CAST(result.s_rate AS bigint) AS s_rate, CAST(result.b_rate AS bigint) AS b_rate -- should be less than 2^64
FROM parse_output_prev4

UNION ALL

-- Withdraw records from regular withdraw transactions (post-v4)
select block_date, tx_hash, trace_id, tx_now, tx_lt, pool_address, pool_name,
result.owner_address, result.sender_address, result.recipient_address, CAST(NULL AS int) AS subaccount_id, result.asset_id,
result.withdraw_amount_current, result.user_new_principal, result.new_total_supply,
result.new_total_borrow,
CAST(result.s_rate AS bigint) AS s_rate, CAST(result.b_rate AS bigint) AS b_rate -- should be less than 2^64
FROM parse_output_postv4

UNION ALL

-- Withdraw records from supply_withdraw transactions
select block_date, tx_hash, trace_id, tx_now, tx_lt, pool_address, pool_name,
owner_address, sender_address, recipient_address, subaccount_id, asset_id,
withdraw_amount_current, user_new_principal, new_total_supply,
new_total_borrow, s_rate, b_rate
FROM withdraw_from_supply_withdraw