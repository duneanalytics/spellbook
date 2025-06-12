{{ config(
       schema = 'stormtrade_ton'
       , alias = 'execute_order'
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


-- execute_order#de1ddbcc is sent from the user position smart contract to the AMM
-- https://github.com/Tsunami-Exchange/storm-contracts-specs/blob/59aefe4/scheme.tlb#L153-L155

WITH valid_amms AS (
    SELECT DISTINCT amm, vault, vault_token FROM {{ ref('stormtrade_ton_trade_notification') }}
),
parsed_boc AS (
    SELECT M.block_date, M.tx_hash, M.trace_id, M.tx_now, M.tx_lt, M.source as user_position, M.destination as amm, vault, vault_token, body_boc
    FROM {{ source('ton', 'messages') }} M
    JOIN valid_amms V ON M.destination = V.amm
    JOIN {{ source('ton', 'transactions') }} T ON M.block_date = T.block_date AND M.tx_hash = T.hash AND M.direction = 'in'
    WHERE M.block_date > TIMESTAMP '2023-10-01'
    {% if is_incremental() %}
        AND {{ incremental_predicate('M.block_date') }}
    {% endif %}
    AND opcode = -568468532 -- execute_order#de1ddbcc 
    AND T.compute_exit_code = 0 AND T.action_result_code = 0 -- only successful transactions
), parse_output as (
select {{ ton_from_boc('body_boc', [
    ton_begin_parse(),
    ton_skip_bits(32),
    ton_load_uint(1, 'direction'),
    ton_load_uint(3, 'order_index'),
    ton_load_address('trader_addr'),
    ton_load_address('prev_addr'),
    ton_load_address('ref_addr'),
    ton_load_uint(32, 'executor_index'),

    ton_load_ref(),
    ton_begin_parse(),
    ton_load_uint(4, 'order_type'),
    ton_load_uint(32, 'order_expiration'),
    ton_load_uint(1, 'order_direction'),
    ton_load_coins('order_amount'),
    ton_load_coins('order_triger_price'),

    ton_restart_parse(),
    ton_begin_parse(),
    ton_load_ref(),
    ton_begin_parse(),
    ton_skip_bits(4 + 32 + 1),
    ton_load_coins('order_amount_tmp'),
    ton_load_uint(64, 'order_leverage'),
    ton_load_coins('order_limit_price'),
    ton_load_coins('order_stop_price'),
    ton_load_coins('order_stop_triger_price'),
    ton_load_coins('order_take_triger_price'),

    ton_restart_parse(),
    ton_begin_parse(),
    ton_skip_refs(1),
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
    ton_skip_refs(2),
    ton_load_ref(),
    ton_begin_parse(),
    ton_load_ref(),
    ton_begin_parse(),
    ton_load_coins('oracle_price'),
    ton_load_coins('oracle_spread'),
    ton_load_uint(32, 'oracle_timestamp'),
    ton_load_uint(16, 'oracle_asset_id'),
    ton_load_uint(32, 'oracle_pause_at'),
    ton_load_uint(32, 'oracle_unpause_at'),
    ton_load_coins('oracle_vpi_spread'),
    ton_load_coins('oracle_vpi_market_depth_long'),
    ton_load_coins('oracle_vpi_market_depth_short'),
    ton_load_uint(64, 'oracle_vpi_k')
    ]) }} as result, * from parsed_boc
)
SELECT block_date, tx_hash, trace_id, tx_now, tx_lt,
user_position, vault, vault_token, amm, result.direction, result.order_index, result.trader_addr, result.prev_addr, result.ref_addr, result.executor_index,
CASE
    WHEN result.order_type = 0 THEN 'stop_loss_order' 
    WHEN result.order_type = 1 THEN 'take_profit_order' 
    WHEN result.order_type = 2 THEN 'stop_limit_order' 
    WHEN result.order_type = 3 THEN 'market_order' 
    ELSE 'unknown_order_type' END as order_type,
result.order_expiration, result.order_direction, result.order_amount,
CASE WHEN result.order_type < 2 THEN result.order_triger_price ELSE NULL END as order_triger_price,
CASE WHEN result.order_type >= 2 THEN result.order_leverage ELSE NULL END as order_leverage,
CASE WHEN result.order_type >= 2 THEN result.order_limit_price ELSE NULL END as order_limit_price,
CASE WHEN result.order_type >= 2 THEN result.order_stop_price ELSE NULL END as order_stop_price,
CASE WHEN result.order_type >= 2 THEN result.order_stop_triger_price ELSE NULL END as order_stop_triger_price,
CASE WHEN result.order_type >= 2 THEN result.order_take_triger_price ELSE NULL END as order_take_triger_price,
result.position_size, result.position_direction, result.position_margin, result.position_open_notional, result.position_last_updated_cumulative_premium,
result.position_fee, result.position_discount, result.position_rebate, result.position_last_updated_timestamp,
result.oracle_price, result.oracle_spread, result.oracle_timestamp, result.oracle_asset_id,
result.oracle_pause_at, result.oracle_unpause_at, result.oracle_vpi_spread, result.oracle_vpi_market_depth_long,
result.oracle_vpi_market_depth_short, result.oracle_vpi_k
FROM parse_output
