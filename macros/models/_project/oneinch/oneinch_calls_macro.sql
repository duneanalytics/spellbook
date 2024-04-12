{% macro 
    oneinch_calls_macro(
        blockchain
    ) 
%}



{% set columns = [
    'blockchain',
    'block_number',
    'block_time',
    'tx_hash',
    'tx_from',
    'tx_to',
    'tx_success',
    'tx_nonce',
    'tx_gas_used',
    'tx_gas_price',
    'tx_priority_fee_per_gas',
    'contract_name',
    'protocol',
    'protocol_version',
    'method',
    'call_selector',
    'call_trace_address',
    'call_from',
    'call_to',
    'call_success',
    'call_gas_used',
    'call_output',
    'call_error',
    'call_type',
    'remains',
    'flags',
] %}
{% set columns = columns | join(', ') %}



with

settlements as (
    select
        fusion_settlement_address as call_from
        , true as fusion
    from ({{ oneinch_blockchain_macro(blockchain) }}), unnest(fusion_settlement_addresses) as t(fusion_settlement_address)
)

, calls as (

    select *
    from (
        select
            {{ columns }}
            , null as maker
            , dst_receiver as receiver
            , src_token_address
            , src_token_amount
            , dst_token_address
            , dst_token_amount
            , cast(null as varbinary) as order_hash
            , false as fusion
        from {{ ref('oneinch_' + blockchain + '_ar') }}

        union all

        select
            {{ columns }}
            , maker
            , receiver
            , maker_asset as src_token_address
            , making_amount as src_token_amount
            , taker_asset as dst_token_address
            , taking_amount as dst_token_amount
            , order_hash
            , coalesce(fusion, false) as fusion
        from {{ ref('oneinch_' + blockchain + '_lop') }}
        left join settlements using(call_from)
        
    )
)

-- output --

select
    blockchain
    , block_number
    , block_time
    , tx_hash
    , tx_from
    , tx_to
    , tx_success
    , tx_nonce
    , tx_gas_used
    , tx_gas_price
    , tx_priority_fee_per_gas
    , contract_name
    , protocol
    , protocol_version
    , method
    , call_selector
    , call_trace_address
    , call_from
    , call_to
    , call_success
    , call_gas_used
    , call_output
    , call_error
    , call_type
    , remains
    , maker
    , receiver
    , src_token_address
    , src_token_amount
    , dst_token_address
    , dst_token_amount
    , order_hash
    , map_concat(flags, map_from_entries(array[('fusion', fusion)])) as flags
from calls

{% endmacro %}