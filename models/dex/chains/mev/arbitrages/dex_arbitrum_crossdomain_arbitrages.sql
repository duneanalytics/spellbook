{{ config(
        schema = 'dex_arbitrum',
        alias = 'crossdomain_arbitrages',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain','tx_hash', 'project_contract_address', 'evt_index']
)
}}

{{dex_crossdomain_arbitrages(
        blockchain='arbitrum'
        , blocks = source('arbitrum','blocks')
        , traces = source('arbitrum','traces')
        , transactions = source('arbitrum','transactions')
)}}
