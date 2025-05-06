{{ config(
       schema = 'evaa_ton'
       , alias = 'liquidate'
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


-- log::liquidate_success is sent by the router and contains all the data regarding the liquidation
-- https://github.com/evaafi/contracts/blob/d5a6bf889f8bbfa8bcc82671c17e65a3b2b360cd/contracts/core/master-liquidate.fc#L353-L377

WITH evaa_ton_pools AS (
    {{ evaa_ton_pools() }}
),
source_data AS (
    SELECT M.block_date, M.tx_hash, M.trace_id, M.tx_now, M.tx_lt, pool_address, pool_name, tx_lt <= v4_upgrate_lt AS is_pre_v4, body_boc
    FROM {{ source('ton', 'messages') }} M
    JOIN evaa_ton_pools ON M.source = pool_address
    WHERE M.direction = 'out' AND M.destination IS NULL -- ext out message
    AND M.block_date >= TIMESTAMP '2023-10-09' -- protocol launch
    {% if is_incremental() %}
        AND {{ incremental_predicate('M.block_date') }}
    {% endif %}
    AND bitwise_right_shift(opcode, 24) = 3 -- log::liquidate_success, 0x3
), parse_output_prev4 as (
select {{ ton_from_boc('body_boc', [
    ton_begin_parse(),
    ton_load_uint(8, 'opcode'),
    ton_load_address('owner_address'),
    ton_load_address('sender_address'),
    ton_load_uint(32, '_current_time'),
    ton_load_ref(),
    ton_begin_parse(),
    ton_load_uint(256, 'transferred_asset_id'),
    ton_load_uint(64, 'transferred_amount'),
    ton_load_int(64, 'new_user_loan_principal'),
    ton_load_int(64, 'loan_new_total_supply'),
    ton_load_int(64, 'loan_new_total_borrow'),
    ton_load_uint(64, 'loan_s_rate'),
    ton_load_uint(64, 'loan_b_rate'),
    ton_restart_parse(),
    ton_begin_parse(),
    ton_skip_refs(1),
    ton_load_ref(),
    ton_begin_parse(),
    ton_load_uint(256, 'collateral_asset_id'),
    ton_load_uint(64, 'collateral_reward'),
    ton_load_int(64, 'new_user_collateral_principal'),
    ton_load_int(64, 'new_collateral_total_supply'),
    ton_load_int(64, 'new_collateral_total_borrow'),
    ton_load_uint(64, 'collateral_s_rate'),
    ton_load_uint(64, 'collateral_b_rate')
    ]) }} as result, * FROM source_data WHERE is_pre_v4
), parse_output_postv4 as (
select {{ ton_from_boc('body_boc', [
    ton_begin_parse(),
    ton_load_uint(8, 'opcode'),
    ton_load_address('owner_address'),
    ton_load_address('sender_address'),
    ton_load_address('liquidator_address'),
    ton_load_uint(32, '_current_time'),
    ton_load_ref(),
    ton_begin_parse(),
    ton_load_uint(256, 'transferred_asset_id'),
    ton_load_uint(64, 'transferred_amount'),
    ton_load_int(64, 'new_user_loan_principal'),
    ton_load_int(64, 'loan_new_total_supply'),
    ton_load_int(64, 'loan_new_total_borrow'),
    ton_load_uint(64, 'loan_s_rate'),
    ton_load_uint(64, 'loan_b_rate'),
    ton_restart_parse(),
    ton_begin_parse(),
    ton_skip_refs(1),
    ton_load_ref(),
    ton_begin_parse(),
    ton_load_uint(256, 'collateral_asset_id'),
    ton_load_uint(64, 'collateral_reward'),
    ton_load_int(64, 'new_user_collateral_principal'),
    ton_load_int(64, 'new_collateral_total_supply'),
    ton_load_int(64, 'new_collateral_total_borrow'),
    ton_load_uint(64, 'collateral_s_rate'),
    ton_load_uint(64, 'collateral_b_rate')
    ]) }} as result, * FROM source_data WHERE NOT is_pre_v4
)
select block_date, tx_hash, trace_id, tx_now, tx_lt, pool_address, pool_name,
result.owner_address, result.sender_address, null AS liquidator_address, result.transferred_asset_id,
result.transferred_amount, result.new_user_loan_principal, result.loan_new_total_supply,
result.loan_new_total_borrow, CAST(result.loan_s_rate AS bigint) AS loan_s_rate, CAST(result.loan_b_rate AS bigint) AS loan_b_rate,-- should be less than 2^64
result.collateral_asset_id, result.collateral_reward, result.new_user_collateral_principal,
result.new_collateral_total_supply, result.new_collateral_total_borrow,
CAST(result.collateral_s_rate AS bigint) AS collateral_s_rate, CAST(result.collateral_b_rate AS bigint) AS collateral_b_rate -- should be less than 2^64
FROM parse_output_prev4

UNION ALL

select block_date, tx_hash, trace_id, tx_now, tx_lt, pool_address, pool_name,
result.owner_address, result.sender_address, result.liquidator_address, result.transferred_asset_id,
result.transferred_amount, result.new_user_loan_principal, result.loan_new_total_supply,
result.loan_new_total_borrow, CAST(result.loan_s_rate AS bigint) AS loan_s_rate, CAST(result.loan_b_rate AS bigint) AS loan_b_rate,-- should be less than 2^64
result.collateral_asset_id, result.collateral_reward, result.new_user_collateral_principal,
result.new_collateral_total_supply, result.new_collateral_total_borrow,
CAST(result.collateral_s_rate AS bigint) AS collateral_s_rate, CAST(result.collateral_b_rate AS bigint) AS collateral_b_rate -- should be less than 2^64
FROM parse_output_postv4