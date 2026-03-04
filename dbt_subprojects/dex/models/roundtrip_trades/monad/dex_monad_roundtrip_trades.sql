{% set blockchain = 'monad' %}

{{ config(
        schema = 'dex_' + blockchain,
        alias = 'roundtrip_trades',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['block_month', 'block_time', 'tx_hash', 'evt_index', 'pool_address'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
)
}}

{{dex_roundtrip_trades(
        blockchain = blockchain
)}}