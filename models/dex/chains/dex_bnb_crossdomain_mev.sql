{{ config(
        tags = ['dunesql'],
        schema = 'dex_bnb',
        alias = alias('crossdomain_mev'),
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain', 'tx_hash', 'project_contract_address', 'evt_index']
)
}}

{{dex_crossdomain_mev(
        blockchain='bnb'
        , transactions = source('bnb','transactions')
)}}
