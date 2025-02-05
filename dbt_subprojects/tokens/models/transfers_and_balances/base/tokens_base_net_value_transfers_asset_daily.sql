{% set blockchain = 'base' %}

{{ config(
        schema = 'tokens_' + blockchain
        , alias = 'net_value_transfers_asset_daily'
        , materialized = 'incremental'
        , file_format = 'delta'
        , incremental_strategy = 'merge'
        , unique_key = ['blockchain', 'block_date', 'contract_address']
        , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')]
        )
}}


{{ evm_net_value_transfers_asset_daily(blockchain) }}