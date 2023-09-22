{{ config(
        tags = ['dunesql'],
        schema = 'dex_avalanche_c',
        alias = alias('sandwiches'),
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain', 'tx_hash', 'project_contract_address', 'evt_index']
)
}}

{{dex_sandwiches(
        blockchain='avalanche_c'
        , transactions = source('avalanche_c','transactions')
)}}