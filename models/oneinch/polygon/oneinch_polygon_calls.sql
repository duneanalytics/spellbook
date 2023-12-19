{% set blockchain = 'polygon' %}



{{ 
    config( 
        schema = 'oneinch_' + blockchain,
        alias = 'calls',
        materialized = 'view',
        unique_key = ['tx_hash', 'call_trace_address']
    )
}}



{{ 
    oneinch_calls_macro(
        blockchain = blockchain
    )
}}
