{%- set blockchain = 'bnb' -%}

{{-
    config(
        schema = 'oneinch_' + blockchain,
        alias = 'project_swaps_base_2024',
        materialized = 'table',
        unique_key = ['blockchain', 'id'],
    )
-}}

-- depends_on: {{ ref('oneinch_' + blockchain + '_project_orders') }}

{{
    oneinch_project_swaps_base_u_macro(
        blockchain = blockchain,
        date_from = '2024-01-01',
        date_to = '2025-01-01'
    )
}}