{{ config(
        tags = ['dunesql'],
        schema = 'dex_fantom',
        alias = 'arbitrages',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain', 'tx_hash', 'evt_index']
)
}}

{{dex_arbitrages(
        blockchain='fantom'
        , transactions = source('fantom','transactions')
)}}