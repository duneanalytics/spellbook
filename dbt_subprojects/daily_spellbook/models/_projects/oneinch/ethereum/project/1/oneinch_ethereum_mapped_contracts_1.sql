{% set blockchain = 'ethereum' %}



{{
    config(
        schema = 'oneinch_' + blockchain,
        alias = 'mapped_contracts_1',
        materialized = 'table',
        unique_key = ['blockchain', 'address'],
    )
}}

{{ oneinch_mapped_contracts_macro_test(blockchain, '1') }}