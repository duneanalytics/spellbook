{% set blockchain = 'arbitrum' %}



{{ 
    config( 
        schema = 'oneinch_' + blockchain,
        alias = 'project_swaps',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        tags=['prod_exclude'],
        unique_key = ['blockchain', 'tx_hash', 'call_trace_address']
    )
}}



{{
    oneinch_project_swaps_macro(
        blockchain = blockchain
    )
}}
