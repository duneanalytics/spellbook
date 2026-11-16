{%- set blockchain = 'fantom' -%}

{{-
    config(
        schema = 'oneinch_' + blockchain,
        alias = 'project_orders',
        partition_by = ['block_month'],
        materialized = 'table',
        tags = ['static'],
        file_format = 'delta',
    )
-}}

-- depends on: {{ ref('oneinch_' + blockchain + '_project_orders_raw_logs') }}
-- depends on: {{ ref('oneinch_' + blockchain + '_project_orders_raw_traces') }}

{{ oneinch_project_orders_macro(blockchain = blockchain) }}