{{ config(
        
        schema = 'generic_uniswap_v2_forks_ethereum',
        alias = 'base_trades',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index', 'token_bought_address', 'token_sold_address'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
        )
}}

{{generic_uniswap_v2_fork(
        blockchain = 'ethereum'
        , transactions = source('ethereum','transactions')
        , logs = source('ethereum','logs')
        , contracts = source('ethereum','contracts')
)}}
