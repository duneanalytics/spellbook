{% set blockchain = 'fantom' %}



{{
    config(
        schema = 'oneinch_' + blockchain,
        alias = 'mapped_contracts',
        materialized = 'table',
        tags = ['static'],
        unique_key = ['blockchain', 'address'],
    )
}}



{{ oneinch_mapped_contracts_macro(blockchain) }}