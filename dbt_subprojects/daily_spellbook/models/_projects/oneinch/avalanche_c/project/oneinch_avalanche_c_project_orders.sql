{%- set blockchain = 'avalanche_c' -%}

{{-
    config(
        tags = ['prod_exclude'],
        schema = 'oneinch_' + blockchain,
        alias = 'project_orders',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        unique_key = ['blockchain', 'block_month', 'block_number', 'tx_hash', 'call_trace_address', 'order_hash', 'call_trade'],
    )
-}}

-- depends on: {{ ref('oneinch_' + blockchain + '_project_orders_raw_logs') }}
-- depends on: {{ ref('oneinch_' + blockchain + '_project_orders_raw_traces') }}

{{ oneinch_project_orders_macro(blockchain = blockchain) }}