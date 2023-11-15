{{ config(
        schema = 'dex_gnosis',
        alias = 'crossdomain_mev',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain', 'tx_hash', 'project_contract_address', 'evt_index']
)
}}

{{dex_crossdomain_mev(
        blockchain='gnosis'
        , blocks = source('gnosis','blocks')
        , traces = source('gnosis','traces')
        , transactions = source('gnosis','transactions')
)}}
