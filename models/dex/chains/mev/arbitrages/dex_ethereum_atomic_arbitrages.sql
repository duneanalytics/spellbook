{{ config(
        schema = 'dex_ethereum',
        alias = 'atomic_arbitrages',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain', 'tx_hash', 'evt_index']
        )
}}

{{dex_atomic_arbitrages(
        blockchain='ethereum'
        , transactions = source('ethereum','transactions')
)}}