{{ config(
        schema = 'uniswap_v3_ethereum',
        alias = 'decoded_pool_evt_swap',
        partition_by = ['block_date'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
        )
}}

{{uniswap_v3_pool_event_decoding(
        logs = source('ethereum', 'logs')
)}}
