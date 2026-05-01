{%- set blockchain = 'bnb' -%}

{{-
    config(
        schema = 'oneinch_' + blockchain,
        alias = 'project_swaps_base_2021_1212',
        materialized = 'table',
        unique_key = ['block_month', 'id'],
        tags = ['static'],
    )
-}}

-- depends_on: {{ ref('oneinch_' + blockchain + '_project_orders') }}

{{
    oneinch_project_swaps_base_macro(
        blockchain = blockchain,
        date_from = '2021-12-01',
        date_to = '2022-01-01'
    )
}}