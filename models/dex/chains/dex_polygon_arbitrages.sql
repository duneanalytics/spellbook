{{ config(
        schema = 'dex_polygon',
        alias = 'arbitrages',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain', 'tx_hash', 'project_contract_address', 'evt_index']
)
}}

{{dex_arbitrages(
        blockchain='polygon'
        , transactions = source('polygon','transactions')
        , fungible_transfers = ref('fungible_arbitrum_transfers')
)}}
