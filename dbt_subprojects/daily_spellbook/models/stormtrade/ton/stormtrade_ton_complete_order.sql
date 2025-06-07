{{ config(
       schema = 'stormtrade_ton'
       , alias = 'complete_order'
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


-- complete_order#cf90d618 is sent from the AMM smart contract to the user position
-- https://github.com/Tsunami-Exchange/storm-contracts-specs/blob/59aefe4/scheme.tlb#L96

WITH valid_amms AS (
    SELECT DISTINCT amm, vault, vault_token FROM {{ ref('stormtrade_ton_trade_notification') }}
),
parsed_boc AS (
    SELECT M.block_date, M.tx_hash, M.trace_id, M.tx_now, M.tx_lt, M.destination as user_position, vault, vault_token, M.source as amm, body_boc
    FROM {{ source('ton', 'messages') }} M
    JOIN valid_amms V ON M.source = V.amm
    JOIN {{ source('ton', 'transactions') }} T ON M.block_date = T.block_date AND M.tx_hash = T.hash AND M.direction = 'in'
    WHERE M.block_date > TIMESTAMP '2023-10-01'
    {% if is_incremental() %}
        AND {{ incremental_predicate('M.block_date') }}
    {% endif %}
    AND opcode = -812591592 -- complete_order#cf90d618
    AND T.compute_exit_code = 0 AND T.action_result_code = 0 -- only successful transactions
), parse_output as (
select {{ ton_from_boc('body_boc', [
    ton_begin_parse(),
    ton_skip_bits(32),
    ton_load_uint(4, 'order_type'),
    ton_load_uint(3, 'order_index'),
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

    ]) }} as result, * from parsed_boc
)
SELECT block_date, tx_hash, trace_id, tx_now, tx_lt,
user_position, vault, vault_token, amm,
CASE
    WHEN result.order_type = 0 THEN 'stop_loss_order' 
    WHEN result.order_type = 1 THEN 'take_profit_order' 
    WHEN result.order_type = 2 THEN 'stop_limit_order' 
    WHEN result.order_type = 3 THEN 'market_order' 
    ELSE 'unknown_order_type' END as order_type,
result.order_index, result.direction, result.origin_op, result.oracle_price, result.settlement_oracle_price,
result.position_size, result.position_direction, result.position_margin, result.position_open_notional,
result.position_last_updated_cumulative_premium, result.position_fee, result.position_discount, result.position_rebate,
result.position_last_updated_timestamp,
result.quote_asset_reserve, result.quote_asset_weight, result.base_asset_reserve,
result.total_long_position_size, result.total_short_position_size,
result.open_interest_long, result.open_interest_short
FROM parse_output