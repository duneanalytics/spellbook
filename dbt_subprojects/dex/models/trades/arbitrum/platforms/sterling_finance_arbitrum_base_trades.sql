{{
    config(
        schema = 'sterling_finance_arbitrum',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{{
    uniswap_compatible_v2_trades(
        blockchain = 'arbitrum',    
        project = 'sterling_finance',
        version = '1',
        Pair_evt_Swap = source('sterling_finance_arbitrum', 'StrPair_STR_WETH_evt_Swap'),
        Factory_evt_PairCreated = source('sterling_finance_arbitrum', 'Factory_evt_PairCreated')
    )
}}
