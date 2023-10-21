{{ config(
        tags = ['dunesql'],
        schema = 'dex_avalanche_c',
        alias = alias('crossdomain_mev'),
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain', 'tx_hash', 'project_contract_address', 'evt_index']
)
}}

{{dex_crossdomain_mev(
        blockchain='avalanche_c'
        , blocks = source('avalanche_c','blocks')
        , traces = source('avalanche_c','traces')
        , transactions = source('avalanche_c','transactions')
)}}
