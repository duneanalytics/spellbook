{% set blockchain = 'ethereum' %}



{{
    config(
        schema = 'oneinch_' + blockchain,
        alias = 'project_swaps',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        unique_key = ['blockchain', 'block_number', 'tx_hash', 'call_trace_address', 'call_trade_id']
    )
}}

-- depends_on: {{ ref('oneinch_' + blockchain + '_project_orders') }}


{{
    oneinch_project_swaps_macro(
        blockchain = blockchain
    )
}}
