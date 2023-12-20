{{  
    config(
        schema = 'oneinch',
        alias = 'lop',
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
        , maker
        , receiver
        , maker_asset
        , making_amount
        , taker_asset
        , taking_amount
        , order_hash
        , remains
        , minute
        , block_month 
    from {{ ref('oneinch_' + blockchain + '_lop') }}
    {% if not loop.last %} union all {% endif %}
{% endfor %}