{% set blockchain = 'avalanche_c' %}



{{ 
    config( 
        schema = 'oneinch_' + blockchain,
        alias = 'ar',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain', 'tx_hash', 'call_trace_address']
    )
}}



{{ 
    oneinch_ar_macro(
        blockchain = blockchain
    )
}}