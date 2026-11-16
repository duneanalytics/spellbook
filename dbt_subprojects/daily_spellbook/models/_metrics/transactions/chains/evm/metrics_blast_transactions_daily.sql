{% set blockchain = 'blast' %}

{{ config(
        schema = 'metrics_' + blockchain
        , alias = 'transactions_daily'
        , materialized = 'table'
        , file_format = 'delta'
        , tags=['static']
        , post_hook='{{ hide_spells() }}'
        )
}}


{{ metrics_transactions_evm(blockchain) }}
