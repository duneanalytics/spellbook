{{  
    config(
        schema = 'oneinch',
        alias = 'ar',
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
        'polygon'
    ]
%}



{% for blockchain in blockchains %}
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
        , priority_fee_per_gas
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
        , src_receiver
        , dst_receiver
        , src_token_address
        , dst_token_address
        , src_amount -- will be removed soon
        , dst_amount -- will be removed soon
        , dst_amount_min -- will be removed soon
        , src_amount as src_token_amount
        , dst_amount as dst_token_amount
        , dst_amount_min as dst_token_amount_min
        , ordinary
        , pools
        , router_type
        , remains
        , minute
        , block_month
    from {{ ref('oneinch_' + blockchain + '_ar') }}
    {% if not loop.last %} union all {% endif %}
{% endfor %}