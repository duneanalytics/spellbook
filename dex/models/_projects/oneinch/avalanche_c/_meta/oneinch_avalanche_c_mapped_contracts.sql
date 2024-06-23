{% set blockchain = 'avalanche_c' %}



{{
    config(
        schema = 'oneinch_' + blockchain,
        alias = 'mapped_contracts',
        materialized = 'table',
        unique_key = ['blockchain', 'address'],
    )
}}



{{ oneinch_mapped_contracts_macro(blockchain) }}