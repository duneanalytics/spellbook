{% set blockchain = 'base' %}

{{ config(
        schema = 'metrics_' + blockchain
        , alias = 'transactions_daily_address'
        , materialized = 'incremental'
        , file_format = 'delta'
        , incremental_strategy = 'merge'
        , unique_key = ['blockchain', 'block_date', 'address']
        , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')]
        )
}}


{{ metrics_transactions_evm_address(blockchain) }}
