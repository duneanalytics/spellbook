{{ config(
        schema = 'dex_polygon',
        alias = 'crossdomain_arbitrages',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain', 'tx_hash', 'project_contract_address', 'evt_index']
)
}}

{{dex_crossdomain_arbitrages(
        blockchain='polygon'
        , blocks = source('polygon','blocks')
        , traces = source('polygon','traces')
        , transactions = source('polygon','transactions')
)}}
