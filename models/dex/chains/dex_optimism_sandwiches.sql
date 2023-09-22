{{ config(
        tags = ['dunesql'],
        schema = 'dex_optimism',
        alias = alias('sandwiches'),
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain', 'tx_hash', 'project_contract_address', 'evt_index']
)
}}

{{dex_sandwiches(
        blockchain='optimism'
        , transactions = source('optimism','transactions')
)}}