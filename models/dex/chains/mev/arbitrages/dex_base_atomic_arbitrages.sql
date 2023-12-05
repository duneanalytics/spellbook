{% set blockchain = 'base' %}

{{ config(
        schema = 'dex_' + blockchain,
        alias = 'atomic_arbitrages',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'project_contract_address', 'evt_index']
)
}}

{{dex_atomic_arbitrages(
        blockchain = blockchain
        , transactions = source(blockchain,'transactions')
)}}