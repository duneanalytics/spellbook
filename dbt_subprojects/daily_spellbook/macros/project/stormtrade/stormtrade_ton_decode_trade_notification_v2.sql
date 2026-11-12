{% macro stormtrade_ton_decode_trade_notification_v2(payload) -%}
{{ ton_from_boc(payload, [
        ton_begin_parse(),
        ton_skip_bits(32),
        ton_load_uint(64, 'query_id'),
        ton_load_uint(16, 'asset_id'),
        ton_load_int(64, 'free_amount'),
        ton_load_int(64, 'locked_amount'),
        ton_load_int(64, 'exchange_amount'),
        ton_load_uint(64, 'withdraw_locked_amount'),
        ton_load_uint(64, 'fee_to_stakers'),
        ton_load_uint(64, 'executor_amount'),
        ton_load_uint(64, 'total_referrer_amount'),
        ton_load_uint(4, 'ref_count'),
        ton_skip_maybe_ref(),
        ton_load_address('trader_addr'),
        ton_load_ref(),
        ton_begin_parse(),
        ton_load_address('origin_addr'),
        ton_load_coins('balance_delta'),
        ton_load_address('amm')
    ]) }}
{%- endmacro %}
