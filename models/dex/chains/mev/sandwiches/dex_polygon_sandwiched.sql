{{ config(
        
        schema = 'dex_polygon',
        alias = 'sandwiched',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain', 'tx_hash', 'project_contract_address', 'evt_index']
)
}}

{{dex_sandwiched(
        blockchain='polygon'
        , transactions = source('polygon','transactions')
        , sandwiches = ref('dex_arbitrum_sandwiches')
)}}
