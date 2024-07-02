{% set blockchain = 'base' %}



{{
    config(
        schema = 'oneinch_' + blockchain,
        alias = 'project_orders',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        unique_key = ['blockchain', 'block_number', 'tx_hash', 'call_trace_address', 'order_hash', 'call_trade']
    )
}}



{{
    oneinch_project_orders_macro(
        blockchain = blockchain
    )
}}