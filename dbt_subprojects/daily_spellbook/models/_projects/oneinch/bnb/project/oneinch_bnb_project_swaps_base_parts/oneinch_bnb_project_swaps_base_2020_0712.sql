{%- set blockchain = 'bnb' -%}

{{-
    config(
        schema = 'oneinch_' + blockchain,
        alias = 'project_swaps_base_2020_0712',
        materialized = 'table',
        unique_key = ['block_month', 'id'],
        tags = ['static'],
    )
-}}

-- depends_on: {{ ref('oneinch_' + blockchain + '_project_orders') }}

{{
    oneinch_project_swaps_base_macro(
        blockchain = blockchain,
        date_from = '2020-07-01',
        date_to = '2021-01-01'
    )
}}