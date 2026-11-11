{% set blockchain = 'blast' %}

{{ config(
        schema = 'metrics_' + blockchain
        , alias = 'gas_fees_daily'
        , materialized = 'table'
        , file_format = 'delta'
        , tags=['static']
        , post_hook='{{ hide_spells() }}'
        )
}}


{{ metrics_fees_evm(blockchain) }}
