{% set blockchain = 'arbitrum' %}



{{
    config(
        schema = 'oneinch',
        alias = 'dict_view_from_table',
        materialized = 'view'
    )
}}



select * from {{ ref('oneinch_test_table') }}