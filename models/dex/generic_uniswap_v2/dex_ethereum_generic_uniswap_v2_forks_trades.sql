{{ config(
        
        schema = 'dex_ethereum',
        alias = 'generic_uniswap_v2_forks',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain', 'tx_hash', 'project_contract_address', 'evt_index']
        )
}}

{{generic_uniswap_v2_fork(
        blockchain = 'ethereum'
        , transactions = source('ethereum','transactions')
        , logs = source('ethereum','logs')
        , contracts = source('ethereum','contracts')
)}}
