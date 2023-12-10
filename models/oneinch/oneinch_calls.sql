{{  
    config(
        schema = 'oneinch',
        alias = 'calls',
        materialized = 'view',
        unique_key = ['blockchain', 'tx_hash', 'call_trace_address']
    )
}}



{% set columns = [
    'blockchain',
    'block_number',
    'block_time',
    'tx_hash',
    'tx_from',
    'tx_to',
    'tx_success',
    'tx_nonce',
    'gas_price',
    'priority_fee',
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
    'remains',
    'explorer_link'
] %}
{% set columns = columns | join(', ') %}



with

info as (
    select
        blockchain
        , explorer_link
    from {{ ref('evms_info') }}
)

, settlements as (
    select
        blockchain
        , contract_address as call_from
        , true as fusion
    from {{ ref('oneinch_fusion_settlements') }}
)

, calls as (

    select *
    from (
        select
            {{ columns }}
            , null as maker
            , dst_receiver as receiver
            , src_token_address
            , src_amount
            , dst_token_address
            , dst_amount
            , false as fusion
            , null as order_hash
        from {{ ref('oneinch_ar') }}
        join info using(blockchain)
        {% if is_incremental() %}
            where {{ incremental_predicate('block_time') }}
        {% endif %}

        union all

        select
            {{ columns }}
            , maker
            , receiver
            , maker_asset as src_token_address
            , making_amount as src_amount
            , taker_asset as dst_token_address
            , taking_amount as dst_amount
            , coalesce(fusion, false) as fusion
            , order_hash
        from {{ ref('oneinch_lop') }}
        join info using(blockchain)
        left join settlements using(blockchain, call_from)
        {% if is_incremental() %}
            where {{ incremental_predicate('block_time') }}
        {% endif %}
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
    , gas_price
    , priority_fee
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
    , remains
    , maker
    , receiver
    , src_token_address
    , src_amount
    , dst_token_address
    , dst_amount
    , fusion
    , order_hash
    , explorer_link
from calls