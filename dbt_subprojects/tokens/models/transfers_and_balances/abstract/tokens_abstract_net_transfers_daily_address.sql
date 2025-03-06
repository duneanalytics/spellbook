{% set blockchain = 'abstract' %}

{{ config(
        schema = 'tokens_' + blockchain
        , alias = 'net_transfers_daily_address'
        , materialized = 'incremental'
        , file_format = 'delta'
        , incremental_strategy = 'merge'
        , unique_key = ['blockchain', 'block_date', 'address_owner']
        , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')]
        )
}}


{{ evm_net_transfers_daily_address(blockchain) }}