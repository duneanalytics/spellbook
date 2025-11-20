{% set blockchain = 'celo' %}

{{ config(
        schema = 'dex_' + blockchain,
        alias = 'multihop_trades',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index', 'project', 'version', 'pool_address', 'token_bought_address', 'token_sold_address'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
)
}}

{{dex_multihop_trades(
        blockchain = blockchain
)}}