{%- set blockchain = 'bnb' -%}

{{-
    config(
        schema = 'oneinch_' + blockchain,
        alias = 'project_swaps_base',
        partition_by = ['block_month', 'project'],
        materialized = 'incremental',
        incremental_strategy = 'microbatch',
        event_time = 'block_time',
        batch_size = 'month',
        begin = '2025-11-20',
        full_refresh = false,
        unique_key = ['blockchain', 'block_month', 'block_number', 'tx_hash', 'second_side', 'call_trace_address', 'call_trade_id'],
    )
-}}

-- depends_on: {{ ref('oneinch_' + blockchain + '_project_orders') }}

{{
    oneinch_project_swaps_base_microbatch_macro(
        blockchain = blockchain,
        date_from = '2025-11-20'
    )
}}