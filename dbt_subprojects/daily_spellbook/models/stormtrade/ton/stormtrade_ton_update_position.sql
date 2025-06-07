{{ config(
       schema = 'stormtrade_ton'
       , alias = 'update_position'
       , materialized = 'incremental'
       , file_format = 'delta'
       , incremental_strategy = 'merge'
       , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')]
       , unique_key = ['tx_hash', 'block_date']
       , post_hook='{{ expose_spells(\'["ton"]\',
                                   "project",
                                   "stormtrade",
                                   \'["pshuvalov"]\') }}'
   )
 }}


-- update_position#60dfc677 is sent from the AMM smart contract to the user position
-- https://github.com/Tsunami-Exchange/storm-contracts-specs/blob/59aefe4/scheme.tlb#L79
-- update_position_with_stop_loss#5d1b17b8 is sent from the AMM smart contract to the user position
-- https://github.com/Tsunami-Exchange/storm-contracts-specs/blob/59aefe4/scheme.tlb#L81

WITH valid_amms AS (
    SELECT DISTINCT amm, vault, vault_token FROM {{ ref('stormtrade_ton_trade_notification') }}
),
parsed_boc_update_position AS (
    SELECT M.block_date, M.tx_hash, M.trace_id, M.tx_now, M.tx_lt, M.destination as user_position, M.source as amm, vault, vault_token, body_boc
    FROM {{ source('ton', 'messages') }} M
    JOIN valid_amms V ON M.source = V.amm
    JOIN {{ source('ton', 'transactions') }} T ON M.block_date = T.block_date AND M.tx_hash = T.hash AND M.direction = 'in'
    WHERE M.block_date > TIMESTAMP '2023-10-01'
    {% if is_incremental() %}
        AND {{ incremental_predicate('M.block_date') }}
    {% endif %}
    AND opcode = 1625278071 -- update_position#60dfc677
    AND T.compute_exit_code = 0 AND T.action_result_code = 0 -- only successful transactions
), parsed_boc_update_position_stop_loss AS (
    SELECT M.block_date, M.tx_hash, M.trace_id, M.tx_now, M.tx_lt, M.destination as user_position, M.source as amm, vault, vault_token, body_boc
    FROM {{ source('ton', 'messages') }} M
    JOIN valid_amms V ON M.source = V.amm
    JOIN {{ source('ton', 'transactions') }} T ON M.block_date = T.block_date AND M.tx_hash = T.hash AND M.direction = 'in'
    WHERE M.block_date > TIMESTAMP '2023-10-01'
    {% if is_incremental() %}
        AND {{ incremental_predicate('M.block_date') }}
    {% endif %}
    AND opcode = 1562056632 -- update_position_with_stop_loss#5d1b17b8
    AND T.compute_exit_code = 0 AND T.action_result_code = 0 -- only successful transactions
), parse_output_update_position as (
select {{ ton_from_boc('body_boc', [
    ton_begin_parse(),
    ton_skip_bits(32),
    ton_load_uint(1, 'direction'),
    ton_load_uint(32, 'origin_op'),
    ton_load_coins('oracle_price'),
    ton_load_coins('settlement_oracle_price'),
    
    ton_load_ref(),
    ton_begin_parse(),
    ton_load_int(128, 'position_size'),
    ton_load_uint(1, 'position_direction'),
    ton_load_coins('position_margin'),
    ton_load_coins('position_open_notional'),
    ton_load_int(64, 'position_last_updated_cumulative_premium'),
    ton_load_uint(32, 'position_fee'),
    ton_load_uint(32, 'position_discount'),
    ton_load_uint(32, 'position_rebate'),
    ton_load_uint(32, 'position_last_updated_timestamp'),

    ton_restart_parse(),
    ton_begin_parse(),
    ton_skip_refs(1),
    ton_load_ref(),
    ton_begin_parse(),
    ton_load_coins('quote_asset_reserve'),
    ton_load_coins('quote_asset_weight'),
    ton_load_coins('base_asset_reserve'),
    ton_load_coins('total_long_position_size'),
    ton_load_coins('total_short_position_size'),
    ton_load_coins('open_interest_long'),
    ton_load_coins('open_interest_short'),

    ]) }} as result, * from parsed_boc_update_position
), parse_output_update_position_stop_loss as (
select {{ ton_from_boc('body_boc', [
    ton_begin_parse(),
    ton_skip_bits(32),
    ton_load_uint(1, 'direction'),
    ton_load_coins('stop_trigger_price'),
    ton_load_coins('take_trigger_price'),
    ton_load_uint(32, 'origin_op'),
    ton_load_coins('oracle_price'),
    ton_load_coins('settlement_oracle_price'),
    
    ton_load_ref(),
    ton_begin_parse(),
    ton_load_int(128, 'position_size'),
    ton_load_uint(1, 'position_direction'),
    ton_load_coins('position_margin'),
    ton_load_coins('position_open_notional'),
    ton_load_int(64, 'position_last_updated_cumulative_premium'),
    ton_load_uint(32, 'position_fee'),
    ton_load_uint(32, 'position_discount'),
    ton_load_uint(32, 'position_rebate'),
    ton_load_uint(32, 'position_last_updated_timestamp'),

    ton_restart_parse(),
    ton_begin_parse(),
    ton_skip_refs(1),
    ton_load_ref(),
    ton_begin_parse(),
    ton_load_coins('quote_asset_reserve'),
    ton_load_coins('quote_asset_weight'),
    ton_load_coins('base_asset_reserve'),
    ton_load_coins('total_long_position_size'),
    ton_load_coins('total_short_position_size'),
    ton_load_coins('open_interest_long'),
    ton_load_coins('open_interest_short'),

    ]) }} as result, * from parsed_boc_update_position_stop_loss
)
SELECT block_date, tx_hash, trace_id, tx_now, tx_lt,
user_position, vault, vault_token, amm,
result.direction, result.origin_op, result.oracle_price, result.settlement_oracle_price, null as stop_trigger_price, null as take_trigger_price,
result.position_size, result.position_direction, result.position_margin, result.position_open_notional,
result.position_last_updated_cumulative_premium, result.position_fee, result.position_discount, result.position_rebate,
result.position_last_updated_timestamp,
result.quote_asset_reserve, result.quote_asset_weight, result.base_asset_reserve,
result.total_long_position_size, result.total_short_position_size,
result.open_interest_long, result.open_interest_short
FROM parse_output_update_position
UNION ALL
SELECT block_date, tx_hash, trace_id, tx_now, tx_lt,
user_position, vault, vault_token, amm,
result.direction, result.origin_op, result.oracle_price, result.settlement_oracle_price, result.stop_trigger_price, result.take_trigger_price,
result.position_size, result.position_direction, result.position_margin, result.position_open_notional,
result.position_last_updated_cumulative_premium, result.position_fee, result.position_discount, result.position_rebate,
result.position_last_updated_timestamp,
result.quote_asset_reserve, result.quote_asset_weight, result.base_asset_reserve,
result.total_long_position_size, result.total_short_position_size,
result.open_interest_long, result.open_interest_short
FROM parse_output_update_position_stop_loss
