{% set blockchain = 'shape' %}

{{ config(
        schema = 'metrics_' + blockchain
        , alias = 'gas_fees_daily'
        , materialized = 'table'
        , tags = ['static']
        , file_format = 'delta'
        )
}}


{{ metrics_fees_evm(blockchain) }}
