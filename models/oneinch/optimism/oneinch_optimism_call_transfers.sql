{% set blockchain = 'optimism' %}



{{ 
    config( 
        schema = 'oneinch_' + blockchain,
        alias = 'call_transfers',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'call_trace_address', 'transfer_trace_address', 'transfer_native']
    )
}}



{{ 
    oneinch_call_transfers_macro(
        blockchain = blockchain
    )
}}
