{% set blockchain = 'base' %}



{{ 
    config( 
        schema = 'oneinch_' + blockchain,
        alias = 'project_orders',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain', 'tx_hash', 'call_trace_address']
    )
}}



{{ 
    oneinch_project_orders_macro(
        blockchain = blockchain
    )
}}