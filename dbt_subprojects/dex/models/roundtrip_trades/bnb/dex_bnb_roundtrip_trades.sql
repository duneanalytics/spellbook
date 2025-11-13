{% set blockchain = 'bnb' %}

{{ config(
        schema = 'dex_' + blockchain,
        alias = 'roundtrip_trades',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'pool_address', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
)
}}

{{dex_roundtrip_trades(
        blockchain = blockchain
)}}