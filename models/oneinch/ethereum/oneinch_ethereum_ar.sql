{% set blockchain = 'ethereum' %}



{{ 
    config( 
        schema = 'oneinch_' + blockchain,
        alias = 'ar',
        depends_on=['dex_raw_pools'],
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        tags=['prod_exclude'],
        unique_key = ['blockchain', 'tx_hash', 'call_trace_address']
    )
}}



{{ 
    oneinch_ar_macro(
        blockchain = blockchain
    )
}}