{% set blockchain = 'gnosis' %}



{{ 
    config( 
        schema = 'oneinch_' + blockchain,
        alias = 'project_orders',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain', 'block_number', 'tx_hash', 'call_trace_address', 'order_hash']
    )
}}



{{ 
    oneinch_project_orders_macro(
        blockchain = blockchain
    )
}}