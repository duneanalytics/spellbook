{{ config(
        schema = 'dex_gnosis',
        alias = 'arbitrages',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain', 'tx_hash', 'project_contract_address', 'evt_index']
)
}}

{{dex_arbitrages(
        blockchain='gnosis'
        , transactions = source('gnosis','transactions')
        , fungible_transfers = ref('fungible_gnosis_transfers')
)}}
