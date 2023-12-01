{{ config(
        schema = 'dex_base',
        alias = 'arbitrages',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain', 'tx_hash', 'evt_index']
)
}}

{{dex_arbitrages(
        blockchain='base'
        , transactions = source('base','transactions')
        , fungible_transfers = ref('fungible_base_transfers')
)}}
