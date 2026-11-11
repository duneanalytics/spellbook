{% set blockchain = 'story' %}

{{ config(
        schema = 'tokens_' + blockchain
        , alias = 'net_transfers_daily'
        , materialized = 'table'
        , tags = ['static']
        , file_format = 'delta'
        )
}}


{{ evm_net_transfers_daily(blockchain) }}
