{{  
    config(
        schema = 'oneinch',
        alias = 'calls',
        materialized = 'view',
        unique_key = ['blockchain', 'tx_hash', 'call_trace_address']
    )
}}



{% 
    set blockchains = [
        'arbitrum', 
        'avalanche_c',
        'base',
        'bnb',
        'ethereum',
        'fantom',
        'gnosis',
        'optimism',
        'polygon',
        'zksync'
    ]
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
    'remains'
] %}
{% set columns = columns | join(', ') %}
{% set native_addresses = '(0x0000000000000000000000000000000000000000, 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee)' %}

with

info as (
    select
        blockchain
        , wrapped_native_token_address as wrapped_address
        , native_token_symbol as native_symbol
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

{% for blockchain in blockchains %}
    select *
    from (
        

        select *
        from (
            select
                {{ columns }}
                , null as maker
                , dst_receiver as receiver
                , if(src_token_address in {{native_addresses}}, wrapped_address, src_token_address) as src_token_address
                , if(src_token_address in {{native_addresses}}, native_symbol) as src_native
                , src_amount
                , if(dst_token_address in {{native_addresses}}, wrapped_address, dst_token_address) as dst_token_address
                , if(dst_token_address in {{native_addresses}}, native_symbol) as dst_native
                , dst_amount
                , false as fusion
                , null as order_hash
                , explorer_link
            from {{ ref('oneinch_' + blockchain + '_ar') }}
            join info using(blockchain)
            left join settlements using(call_from)
            {% if is_incremental() %}
                where {{ incremental_predicate('block_time') }}
            {% endif %}

            union all

            select
                {{ columns }}
                , maker
                , receiver
                , if(maker_asset in {{native_addresses}}, wrapped_address, maker_asset) as src_token_address
                , if(maker_asset in {{native_addresses}}, native_symbol) as src_native
                , making_amount as src_amount
                , if(taker_asset in {{native_addresses}}, wrapped_address, taker_asset) as dst_token_address
                , if(taker_asset in {{native_addresses}}, native_symbol) as dst_native
                , taking_amount as dst_amount
                , coalesce(fusion, false) as fusion
                , order_hash
                , explorer_link
            from {{ ref('oneinch_' + blockchain + '_lop') }}
            join info using(blockchain)
            left join settlements using(blockchain, call_from)
            {% if is_incremental() %}
                where {{ incremental_predicate('block_time') }}
            {% endif %}
        )
    )
    {% if not loop.last %}
        union all
    {% endif %}
{% endfor %}