{{ config(
        schema = 'mass_decoding_arbitrum',
        alias = 'uniswap_v2_factory_evt_PairCreated',
        partition_by = ['block_date'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
        )
}}

{{uniswap_v2_factory_event_decoding(
        logs = source('arbitrum', 'logs')
)}}
