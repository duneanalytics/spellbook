{% macro oneinch_cc_macro(blockchain) %}

{% set stream = 'cc' %}
{% set meta = oneinch_meta_cfg_macro(property = 'blockchains') %}
{% set contracts = oneinch_meta_cfg_macro(property = 'streams')[stream]['contracts'] %}
{% set stream_start = oneinch_meta_cfg_macro(property = 'streams')[stream]['start'] %}
{% set date_from = [meta['start'][blockchain], stream_start] | max %}
{% set wrapper = meta['wrapped_native_token_address'][blockchain] %}



with

iterations as (
    select *
    from ({{ oneinch_cc_handle_src_creations_macro(blockchain, stream, contracts, date_from) }})
    
    union all

    select *
    from ({{ oneinch_cc_handle_dst_creations_macro(blockchain, contracts, date_from) }})

    union all
    
    select *
    from ({{ oneinch_cc_handle_results_macro(blockchain, contracts, date_from) }})
)

, native_prices as (
    select
        minute
        , price
        , decimals
    from {{ source('prices', 'usd') }}
    where
        blockchain = '{{ blockchain }}'
        and contract_address = {{ wrapper }}
        and minute >= timestamp '{{ date_from }}'
        {% if is_incremental() %}and {{ incremental_predicate('minute') }}{% endif %}
)

-- output --

select
    blockchain
    , chain_id
    , block_number
    , block_time
    , tx_hash
    , tx_success
    , tx_from
    , tx_to
    , tx_nonce
    , tx_gas_used
    , tx_gas_price
    , tx_priority_fee_per_gas
    , tx_index -- it is necessary to determine the order of creations in the block
    , call_trace_address
    , call_success
    , call_gas_used
    , call_selector
    , call_method
    , call_from
    , call_to
    , call_output
    , call_error
    , call_type
    , 'CC' as protocol
    , protocol_version
    , contract_address
    , contract_name
    , action
    , action_id
    , order_hash
    , hashlock
    , escrow
    , secret
    , maker
    , taker
    , receiver
    , token
    , amount
    , safety_deposit
    , timelocks
    , complement
    , remains
    , flags
    , minute
    , block_date
    , block_month
    , price as native_price
    , decimals as native_decimals
from ({{
    add_tx_columns(
        model_cte = 'iterations'
        , blockchain = blockchain
        , columns = ['from', 'to', 'success', 'nonce', 'gas_price', 'priority_fee_per_gas', 'gas_used', 'index']
    )
}}) as t
left join native_prices using(minute)

{% endmacro %}