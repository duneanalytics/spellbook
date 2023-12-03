{{ config(
        schema = 'dex_avalanche_c',
        alias = 'arbitrages',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain', 'tx_hash', 'evt_index']
)
}}

{{dex_arbitrages(
        blockchain='avalanche_c'
        , transactions = source('avalanche_c','transactions')
)}}