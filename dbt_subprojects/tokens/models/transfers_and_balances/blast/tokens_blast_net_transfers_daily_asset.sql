{% set blockchain = 'blast' %}

{{ config(
        schema = 'tokens_' + blockchain
        , alias = 'net_transfers_daily_asset'
        , materialized = 'incremental'
        , file_format = 'delta'
        , incremental_strategy = 'merge'
        , unique_key = ['blockchain', 'block_date', 'contract_address']
        , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')]
        )
}}


{{ evm_net_transfers_daily_asset(
        blockchain=blockchain,
        native_contract_address=var('0xb1a5700fA2358173Fe465e6eA4Ff52E36e88E2ad')
) 
}}