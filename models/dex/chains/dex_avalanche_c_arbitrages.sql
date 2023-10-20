{{ config(
        tags = ['dunesql'],
        schema = 'dex_avalanche_c',
        alias = alias('arbitrages'),
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain', 'tx_hash', 'project_contract_address', 'evt_index']
)
}}

{{dex_arbitrages(
        blockchain='avalanche_c'
        , transactions = source('avalanche_c','transactions')
        , fungible_transfers = ref('fungible_avalanche_c_transfers')
)}}
