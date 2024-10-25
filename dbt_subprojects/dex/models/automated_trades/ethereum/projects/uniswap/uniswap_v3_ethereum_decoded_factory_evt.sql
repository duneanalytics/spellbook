{{ config(
        schema = 'uniswap_v3_ethereum',
        alias = 'decoded_factory_evt',
        partition_by = ['block_date'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
        )
}}

{{uniswap_v3_factory_event_decoding(
        logs = source('ethereum', 'logs')
)}}

