{{ config(

        schema = 'dex_mass_decoding_arbitrum',
        alias = 'uniswap_v3_pool_evt_Swap',
        partition_by = ['block_date'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
        )
}}

{{uniswap_v3_pool_mass_decoding(
        logs = source('arbitrum', 'logs')
)}}