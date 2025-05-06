{% set blockchain = 'bnb' %}

{{ config(
        schema = 'tokens_' + blockchain
        , alias = 'net_transfers_daily'
        , materialized = 'incremental'
        , file_format = 'delta'
        , incremental_strategy = 'merge'
        , unique_key = ['blockchain', 'block_date']
        , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')]
        )
}}


{{ evm_net_transfers_daily(blockchain) }}