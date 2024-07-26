{% set blockchain = 'celo' %}

{{ config(
        schema = 'dex_' + blockchain,
        alias = 'atomic_arbitrages',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'project_contract_address', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
)
}}

{{dex_atomic_arbitrages(
        blockchain = blockchain
        , transactions = source(blockchain,'transactions')
)}}