{% set blockchain = 'sophon' %}

{{ config(
        schema = 'metrics_' + blockchain
        , alias = 'transactions_daily'
        , materialized = 'table'
        , tags = ['static']
        , file_format = 'delta'
        )
}}


{{ metrics_transactions_evm(blockchain) }}
