{% set blockchain = 'scroll' %}

{{ config(
        
        schema = 'gas_' + blockchain,
        alias = 'prices',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['minute']
)
}}

{{gas_prices(
        blockchain = blockchain
        , transactions = source(blockchain, 'transactions')
)}}