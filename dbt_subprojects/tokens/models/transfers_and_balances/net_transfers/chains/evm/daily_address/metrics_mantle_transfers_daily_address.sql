{% set blockchain = 'mantle' %}

{{ config(
        schema = 'metrics_' + blockchain
        , alias = 'transfers_daily_address'
        , materialized = 'incremental'
        , file_format = 'delta'
        , incremental_strategy = 'merge'
        , unique_key = ['blockchain', 'block_date', 'address']
        , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')]
        )
}}


{{ metrics_transfers_evm_address(blockchain) }}
